from __future__ import annotations

import contextlib
import math
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

import structlog

from backtestforecast.backtests.run_warnings import make_warning
from backtestforecast.backtests.rules import EntryRuleComputationCache
from backtestforecast.config import get_settings
from backtestforecast.errors import DataUnavailableError, ExternalServiceError
from backtestforecast.backtests.strategies.common import preferred_expiration_dates
from backtestforecast.db.session import create_readonly_session, create_session
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.contract_catalog import OptionContractCatalogStore
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord, OptionSnapshotRecord
from backtestforecast.models import HistoricalEarningsEvent, HistoricalExDividendDate, HistoricalOptionContractCatalogSnapshot
from backtestforecast.observability.metrics import MARKET_DATA_CACHE_HITS, MARKET_DATA_CACHE_MISSES
from backtestforecast.pipeline.regime import MIN_BARS as REGIME_MIN_BARS
from backtestforecast.schemas.backtests import (
    AdxSeries,
    AvoidEarningsRule,
    BollingerBandSeries,
    BollingerBandsRule,
    CciSeries,
    CloseSeries,
    CreateBacktestRunRequest,
    EmaSeries,
    IndicatorLevelCrossRule,
    IndicatorPersistenceRule,
    IndicatorSeries,
    IndicatorSeriesCrossRule,
    IndicatorThresholdRule,
    IndicatorTrendRule,
    IvPercentileRule,
    IvPercentileSeries,
    IvRankRule,
    IvRankSeries,
    MacdHistogramSeries,
    MacdLineSeries,
    MacdRule,
    MacdSignalSeries,
    MfiSeries,
    MovingAverageCrossoverRule,
    RegimeRule,
    RocSeries,
    RsiRule,
    RsiSeriesSpec,
    SmaSeries,
    StochasticDSeries,
    StochasticKSeries,
    SupportResistanceRule,
    VolumeRatioSeries,
    VolumeSpikeRule,
    WilliamsRSeries,
)
from backtestforecast.utils.dates import is_trading_day, market_date_today

logger = structlog.get_logger("market_data")

if TYPE_CHECKING:
    from backtestforecast.market_data.redis_cache import OptionDataRedisCache


@dataclass(frozen=True, slots=True)
class HistoricalDataBundle:
    bars: list[DailyBar]
    earnings_dates: set[date]
    ex_dividend_dates: set[date]
    option_gateway: Any
    data_source: str = "massive"
    warnings: list[dict[str, Any]] | None = None
    prefetched_signatures: set[tuple[Any, ...]] = field(default_factory=set, compare=False, repr=False)
    prefetched_summaries: dict[tuple[Any, ...], dict[str, Any]] = field(default_factory=dict, compare=False, repr=False)
    prefetch_lock: threading.RLock = field(default_factory=threading.RLock, compare=False, repr=False)
    prefetch_inflight: dict[tuple[Any, ...], threading.Event] = field(default_factory=dict, compare=False, repr=False)
    entry_rule_cache: EntryRuleComputationCache = field(default_factory=EntryRuleComputationCache, compare=False, repr=False)

    def has_prefetched(self, signature: tuple[Any, ...]) -> bool:
        with self.prefetch_lock:
            return signature in self.prefetched_signatures

    def get_prefetch_summary(self, signature: tuple[Any, ...]) -> dict[str, Any] | None:
        with self.prefetch_lock:
            summary = self.prefetched_summaries.get(signature)
            return dict(summary) if summary is not None else None

    def remember_prefetch(self, signature: tuple[Any, ...], summary: dict[str, Any]) -> None:
        with self.prefetch_lock:
            self.prefetched_signatures.add(signature)
            self.prefetched_summaries[signature] = dict(summary)

    def begin_prefetch(
        self,
        signature: tuple[Any, ...],
    ) -> tuple[str, dict[str, Any] | None, threading.Event | None]:
        with self.prefetch_lock:
            summary = self.prefetched_summaries.get(signature)
            if summary is not None or signature in self.prefetched_signatures:
                return "cached", (dict(summary) if summary is not None else {}), None
            inflight_event = self.prefetch_inflight.get(signature)
            if inflight_event is None:
                inflight_event = threading.Event()
                self.prefetch_inflight[signature] = inflight_event
                return "run", None, inflight_event
            return "wait", None, inflight_event

    def end_prefetch(
        self,
        signature: tuple[Any, ...],
        summary: dict[str, Any] | None,
    ) -> None:
        with self.prefetch_lock:
            if summary is not None:
                self.prefetched_signatures.add(signature)
                self.prefetched_summaries[signature] = dict(summary)
            inflight_event = self.prefetch_inflight.pop(signature, None)
        if inflight_event is not None:
            inflight_event.set()

    def clone_for_execution(self) -> HistoricalDataBundle:
        return HistoricalDataBundle(
            bars=self.bars,
            earnings_dates=self.earnings_dates,
            ex_dividend_dates=self.ex_dividend_dates,
            option_gateway=self.option_gateway,
            data_source=self.data_source,
            warnings=list(self.warnings) if self.warnings is not None else None,
            prefetched_signatures=self.prefetched_signatures,
            prefetched_summaries=self.prefetched_summaries,
            prefetch_lock=self.prefetch_lock,
            prefetch_inflight=self.prefetch_inflight,
            entry_rule_cache=EntryRuleComputationCache(),
        )


@dataclass(frozen=True, slots=True)
class ExDividendLoadResult:
    dates: set[date]
    warnings: list[dict[str, Any]] | None = None


@dataclass(frozen=True, slots=True)
class _BarsFetchResult:
    bars: list[DailyBar]
    local_history_complete: bool = False
    local_history_start: date | None = None


def historical_flatfile_pricing_warning() -> dict[str, Any]:
    return make_warning(
        "historical_aggregate_close_pricing",
        (
            "Historical flat-file mode uses option daily aggregate close as a quote proxy instead "
            "of quote-mid or full NBBO history. Treat fills, marks, and spread economics as approximate."
        ),
        severity="warning",
        metadata={
            "data_source": "historical_flatfile",
            "degraded_mode": True,
        },
    )


_GATEWAY_CONTRACT_CACHE_MAX = 2_000
_GATEWAY_QUOTE_CACHE_MAX = 10_000
_GATEWAY_SNAPSHOT_CACHE_MAX = 5_000
_GATEWAY_IV_CACHE_MAX = 50_000
_EX_DIVIDEND_CACHE_MAX = 1_000

_global_cache_entries = 0
_global_cache_lock = threading.Lock()
_GLOBAL_CACHE_BUDGET = 50_000


def get_global_cache_entries() -> int:
    """Return the approximate number of entries across all gateway caches."""
    return _global_cache_entries


def _filter_contracts_by_strike_bounds(
    contracts: list[OptionContractRecord],
    *,
    strike_floor: float | None,
    strike_ceiling: float | None,
) -> list[OptionContractRecord]:
    if strike_floor is None and strike_ceiling is None:
        return list(contracts)
    return [
        contract
        for contract in contracts
        if (strike_floor is None or contract.strike_price >= strike_floor)
        and (strike_ceiling is None or contract.strike_price <= strike_ceiling)
    ]


class MassiveOptionGateway:
    def __init__(
        self,
        client: MassiveClient,
        symbol: str,
        redis_cache: OptionDataRedisCache | None = None,
        contract_catalog: OptionContractCatalogStore | None = None,
    ) -> None:
        self.client = client
        self.symbol = symbol
        self._redis_cache = redis_cache
        self._contract_catalog = contract_catalog
        self._contract_cache: OrderedDict[tuple[date, str, int, int], list[OptionContractRecord]] = OrderedDict()
        self._exact_contract_cache: OrderedDict[
            tuple[date, str, date, float | None, float | None],
            list[OptionContractRecord],
        ] = OrderedDict()
        self._quote_cache: OrderedDict[tuple[str, date], OptionQuoteRecord | None] = OrderedDict()
        self._snapshot_cache: OrderedDict[str, OptionSnapshotRecord | None] = OrderedDict()
        self._iv_cache: OrderedDict[tuple[str, date], float | None] = OrderedDict()
        self._chain_snapshot_loaded: bool = False
        self._lock = threading.RLock()
        self._contracts_inflight: dict[tuple[date, str, int, int], threading.Event] = {}
        self._quotes_inflight: dict[tuple[str, date], threading.Event] = {}
        self._inflight_errors: dict[tuple[str, object], tuple[Exception, float]] = {}
        self._tracked_entries = 0
        self._ex_dividend_dates: set[date] = set()

    def _track_add(self, count: int = 1) -> None:
        global _global_cache_entries
        with self._lock:
            self._tracked_entries = max(0, self._tracked_entries + count)
        with _global_cache_lock:
            _global_cache_entries = max(0, _global_cache_entries + count)

    def _track_remove(self, count: int = 1) -> None:
        global _global_cache_entries
        with self._lock:
            self._tracked_entries = max(0, self._tracked_entries - count)
        with _global_cache_lock:
            _global_cache_entries = max(0, _global_cache_entries - count)

    def _is_over_budget(self) -> bool:
        return _global_cache_entries > _GLOBAL_CACHE_BUDGET

    def clear_caches(self) -> None:
        """Release all in-memory cache entries for this gateway."""
        with self._lock:
            contract_items = sum(max(len(v), 1) for v in self._contract_cache.values())
            exact_contract_items = sum(max(len(v), 1) for v in self._exact_contract_cache.values())
            total = (
                contract_items + exact_contract_items + len(self._quote_cache)
                + len(self._snapshot_cache) + len(self._iv_cache)
            )
            self._contract_cache.clear()
            self._exact_contract_cache.clear()
            self._quote_cache.clear()
            self._snapshot_cache.clear()
            self._iv_cache.clear()
            self._chain_snapshot_loaded = False
            self._track_remove(total)

    def store_iv(self, key: tuple[str, date], value: float | None) -> None:
        """Store an IV estimate in the bounded LRU cache (thread-safe)."""
        with self._lock:
            if key in self._iv_cache:
                self._iv_cache.move_to_end(key)
                self._iv_cache[key] = value
                return
            if len(self._iv_cache) >= _GATEWAY_IV_CACHE_MAX:
                evicted = 0
                for _ in range(len(self._iv_cache) // 4):
                    self._iv_cache.popitem(last=False)
                    evicted += 1
                self._track_remove(evicted)
            self._iv_cache[key] = value
            self._track_add(1)

    def get_iv(self, key: tuple[str, date]) -> tuple[bool, float | None]:
        """Look up an IV estimate. Returns (found, value)."""
        with self._lock:
            if key in self._iv_cache:
                self._iv_cache.move_to_end(key)
                return True, self._iv_cache[key]
        return False, None

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> list[OptionContractRecord]:
        cache_key = (entry_date, contract_type, target_dte, dte_tolerance_days)
        with self._lock:
            contracts = self._contract_cache.get(cache_key)
            if contracts is not None:
                self._contract_cache.move_to_end(cache_key)
                return contracts
            inflight_event = self._contracts_inflight.get(cache_key)
            if inflight_event is None:
                inflight_event = threading.Event()
                self._contracts_inflight[cache_key] = inflight_event
                am_fetcher = True
            else:
                am_fetcher = False

        if not am_fetcher:
            inflight_event.wait(timeout=30)
            with self._lock:
                contracts = self._contract_cache.get(cache_key)
                if contracts is not None:
                    self._contract_cache.move_to_end(cache_key)
                    return contracts
                error = self._inflight_errors.get(("contracts", cache_key))
            if error is not None:
                raise error[0]

        expiration_gte = entry_date + timedelta(days=max(1, target_dte - dte_tolerance_days))
        expiration_lte = entry_date + timedelta(days=target_dte + dte_tolerance_days)

        try:
            if self._redis_cache is not None:
                cached = self._redis_cache.get_contracts(
                    self.symbol, entry_date, contract_type, expiration_gte, expiration_lte,
                )
                if cached is not None:
                    MARKET_DATA_CACHE_HITS.labels(cache_type="contracts").inc()
                    contracts = [c for c in cached if c.shares_per_contract == 100]
                    self._store_contracts_in_memory(cache_key, contracts)
                    return contracts
                MARKET_DATA_CACHE_MISSES.labels(cache_type="contracts").inc()

            contracts = self.client.list_option_contracts(
                symbol=self.symbol,
                as_of_date=entry_date,
                contract_type=contract_type,
                expiration_gte=expiration_gte,
                expiration_lte=expiration_lte,
            )

            contracts = [contract for contract in contracts if contract.shares_per_contract == 100]

            if self._redis_cache is not None:
                from backtestforecast.market_data.redis_cache import _NEGATIVE_CACHE_TTL_SECONDS

                self._redis_cache.set_contracts(
                    self.symbol,
                    entry_date,
                    contract_type,
                    expiration_gte,
                    expiration_lte,
                    contracts,
                    ttl_seconds=_NEGATIVE_CACHE_TTL_SECONDS if not contracts else None,
                )

            self._store_contracts_in_memory(cache_key, contracts)
            with self._lock:
                self._inflight_errors.pop(("contracts", cache_key), None)
            return contracts
        except Exception as exc:
            with self._lock:
                self._inflight_errors[("contracts", cache_key)] = (exc, time.monotonic())
            raise
        finally:
            with self._lock:
                self._contracts_inflight.pop(cache_key, None)
                inflight_event.set()

    def list_contracts_for_preferred_expiration(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
        *,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[OptionContractRecord]:
        for expiration_date in preferred_expiration_dates(entry_date, target_dte, dte_tolerance_days):
            contracts = self._list_contracts_for_exact_expiration(
                entry_date=entry_date,
                contract_type=contract_type,
                expiration_date=expiration_date,
                strike_price_gte=strike_price_gte,
                strike_price_lte=strike_price_lte,
            )
            if contracts:
                return contracts
        raise DataUnavailableError("No eligible option expirations were available.")

    def list_contracts_for_expiration(
        self,
        *,
        entry_date: date,
        contract_type: str,
        expiration_date: date,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[OptionContractRecord]:
        return self._list_contracts_for_exact_expiration(
            entry_date=entry_date,
            contract_type=contract_type,
            expiration_date=expiration_date,
            strike_price_gte=strike_price_gte,
            strike_price_lte=strike_price_lte,
        )

    def list_contracts_for_expirations(
        self,
        *,
        entry_date: date,
        contract_type: str,
        expiration_dates: list[date],
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> dict[date, list[OptionContractRecord]]:
        if not expiration_dates:
            return {}
        requested_expirations = list(dict.fromkeys(expiration_dates))
        return {
            expiration_date: self._list_contracts_for_exact_expiration(
                entry_date=entry_date,
                contract_type=contract_type,
                expiration_date=expiration_date,
                strike_price_gte=strike_price_gte,
                strike_price_lte=strike_price_lte,
            )
            for expiration_date in requested_expirations
        }

    def _list_contracts_for_exact_expiration(
        self,
        *,
        entry_date: date,
        contract_type: str,
        expiration_date: date,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[OptionContractRecord]:
        strike_floor = round(strike_price_gte, 4) if strike_price_gte is not None else None
        strike_ceiling = round(strike_price_lte, 4) if strike_price_lte is not None else None
        cache_key = (entry_date, contract_type, expiration_date, strike_floor, strike_ceiling)
        with self._lock:
            contracts = self._exact_contract_cache.get(cache_key)
            if contracts is not None:
                self._exact_contract_cache.move_to_end(cache_key)
                return contracts
            if strike_floor is not None or strike_ceiling is not None:
                full_cache_key = (entry_date, contract_type, expiration_date, None, None)
                full_contracts = self._exact_contract_cache.get(full_cache_key)
                if full_contracts is not None:
                    self._exact_contract_cache.move_to_end(full_cache_key)
                    filtered_contracts = _filter_contracts_by_strike_bounds(
                        full_contracts,
                        strike_floor=strike_floor,
                        strike_ceiling=strike_ceiling,
                    )
                    self._store_exact_contracts_in_memory(cache_key, filtered_contracts)
                    return filtered_contracts

        use_redis = self._redis_cache is not None and strike_floor is None and strike_ceiling is None
        if self._contract_catalog is not None:
            cached = self._contract_catalog.get_contracts(
                symbol=self.symbol,
                as_of_date=entry_date,
                contract_type=contract_type,
                expiration_date=expiration_date,
                strike_price_gte=strike_floor,
                strike_price_lte=strike_ceiling,
            )
            if cached is not None:
                self._store_exact_contracts_in_memory(cache_key, cached)
                return cached
        if use_redis:
            cached = self._redis_cache.get_contracts(
                self.symbol, entry_date, contract_type, expiration_date, expiration_date,
            )
            if cached is not None:
                MARKET_DATA_CACHE_HITS.labels(cache_type="contracts").inc()
                contracts = [c for c in cached if c.shares_per_contract == 100]
                self._store_exact_contracts_in_memory(cache_key, contracts)
                return contracts
            MARKET_DATA_CACHE_MISSES.labels(cache_type="contracts").inc()

        contracts = self.client.list_option_contracts_for_expiration(
            symbol=self.symbol,
            as_of_date=entry_date,
            contract_type=contract_type,
            expiration_date=expiration_date,
            strike_price_gte=strike_floor,
            strike_price_lte=strike_ceiling,
        )
        contracts = [contract for contract in contracts if contract.shares_per_contract == 100]

        if self._contract_catalog is not None:
            self._contract_catalog.upsert_contracts(
                symbol=self.symbol,
                as_of_date=entry_date,
                contract_type=contract_type,
                expiration_date=expiration_date,
                strike_price_gte=strike_floor,
                strike_price_lte=strike_ceiling,
                contracts=contracts,
            )

        if use_redis:
            from backtestforecast.market_data.redis_cache import _NEGATIVE_CACHE_TTL_SECONDS

            self._redis_cache.set_contracts(
                self.symbol,
                entry_date,
                contract_type,
                expiration_date,
                expiration_date,
                contracts,
                ttl_seconds=_NEGATIVE_CACHE_TTL_SECONDS if not contracts else None,
            )

        self._store_exact_contracts_in_memory(cache_key, contracts)
        return contracts

    def _store_contracts_in_memory(
        self,
        cache_key: tuple[date, str, int, int],
        contracts: list[OptionContractRecord],
    ) -> None:
        item_count = max(len(contracts), 1)
        with self._lock:
            if len(self._contract_cache) >= _GATEWAY_CONTRACT_CACHE_MAX or self._is_over_budget():
                evicted_items = 0
                for _ in range(len(self._contract_cache) // 4):
                    _, evicted_list = self._contract_cache.popitem(last=False)
                    evicted_items += max(len(evicted_list), 1)
                self._track_remove(evicted_items)
            self._contract_cache[cache_key] = contracts
            self._track_add(item_count)

    def _store_exact_contracts_in_memory(
        self,
        cache_key: tuple[date, str, date, float | None, float | None],
        contracts: list[OptionContractRecord],
    ) -> None:
        item_count = max(len(contracts), 1)
        with self._lock:
            if len(self._exact_contract_cache) >= _GATEWAY_CONTRACT_CACHE_MAX or self._is_over_budget():
                evicted_items = 0
                for _ in range(max(1, len(self._exact_contract_cache) // 4)):
                    _, evicted_list = self._exact_contract_cache.popitem(last=False)
                    evicted_items += max(len(evicted_list), 1)
                self._track_remove(evicted_items)
            self._exact_contract_cache[cache_key] = contracts
            self._track_add(item_count)

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        cache_key = (option_ticker, trade_date)
        with self._lock:
            if cache_key in self._quote_cache:
                self._quote_cache.move_to_end(cache_key)
                return self._quote_cache[cache_key]
            inflight_event = self._quotes_inflight.get(cache_key)
            if inflight_event is None:
                inflight_event = threading.Event()
                self._quotes_inflight[cache_key] = inflight_event
                am_fetcher = True
            else:
                am_fetcher = False

        if not am_fetcher:
            inflight_event.wait(timeout=30)
            with self._lock:
                if cache_key in self._quote_cache:
                    self._quote_cache.move_to_end(cache_key)
                    return self._quote_cache[cache_key]
                error = self._inflight_errors.get(("quotes", cache_key))
            if error is not None:
                raise error[0]

        try:
            if self._redis_cache is not None:
                from backtestforecast.market_data.redis_cache import CACHE_MISS
                redis_result = self._redis_cache.get_quote(option_ticker, trade_date)
                if redis_result is not CACHE_MISS:
                    MARKET_DATA_CACHE_HITS.labels(cache_type="quotes").inc()
                    self._store_quote_in_memory(cache_key, redis_result)
                    return redis_result
                MARKET_DATA_CACHE_MISSES.labels(cache_type="quotes").inc()

            quote = self.client.get_option_quote_for_date(option_ticker, trade_date)

            if self._redis_cache is not None:
                from backtestforecast.market_data.redis_cache import _NEGATIVE_CACHE_TTL_SECONDS

                self._redis_cache.set_quote(
                    option_ticker,
                    trade_date,
                    quote,
                    ttl_seconds=_NEGATIVE_CACHE_TTL_SECONDS if quote is None else None,
                )

            self._store_quote_in_memory(cache_key, quote)
            with self._lock:
                self._inflight_errors.pop(("quotes", cache_key), None)
            return quote
        except Exception as exc:
            with self._lock:
                self._inflight_errors[("quotes", cache_key)] = (exc, time.monotonic())
            raise
        finally:
            with self._lock:
                self._quotes_inflight.pop(cache_key, None)
                inflight_event.set()

    def set_ex_dividend_dates(self, ex_dividend_dates: set[date]) -> None:
        """Store ex-dividend dates on the live option gateway.

        This is part of the production backtest path: ``prepare_backtest()``
        loads dates from the market-data client and injects them here before
        the engine starts requesting assignment-relevant windows.
        """
        self._ex_dividend_dates = set(ex_dividend_dates)

    def get_ex_dividend_dates(self, start_date: date, end_date: date) -> set[date]:
        return {
            ex_date for ex_date in self._ex_dividend_dates
            if start_date <= ex_date <= end_date
        }

    def _store_quote_in_memory(
        self,
        cache_key: tuple[str, date],
        quote: OptionQuoteRecord | None,
    ) -> None:
        with self._lock:
            if len(self._quote_cache) >= _GATEWAY_QUOTE_CACHE_MAX or self._is_over_budget():
                evicted = 0
                for _ in range(len(self._quote_cache) // 4):
                    self._quote_cache.popitem(last=False)
                    evicted += 1
                self._track_remove(evicted)
            self._quote_cache[cache_key] = quote
            self._track_add(1)

    def get_snapshot(self, option_ticker: str) -> OptionSnapshotRecord | None:
        """Return a real-time snapshot (greeks, IV) for a single option contract.

        Results are cached for the lifetime of the gateway.
        """
        with self._lock:
            if option_ticker in self._snapshot_cache:
                self._snapshot_cache.move_to_end(option_ticker)
                return self._snapshot_cache[option_ticker]
        snapshot = self.client.get_option_snapshot(self.symbol, option_ticker)
        with self._lock:
            if len(self._snapshot_cache) >= _GATEWAY_SNAPSHOT_CACHE_MAX or self._is_over_budget():
                evicted = 0
                for _ in range(len(self._snapshot_cache) // 4):
                    self._snapshot_cache.popitem(last=False)
                    evicted += 1
                self._track_remove(evicted)
            self._snapshot_cache[option_ticker] = snapshot
            self._track_add(1)
        return snapshot

    def get_chain_delta_lookup(
        self,
        contracts: list[OptionContractRecord],
    ) -> dict[tuple[float, date], float]:
        """Build a (strike, expiration)->delta lookup using the chain snapshot.

        Fetches the full option chain snapshot once, caches individual results,
        and returns a mapping of ``(strike_price, expiration_date)`` to absolute
        delta. Keying by both strike and expiration prevents incorrect delta
        assignment when multiple expirations share the same strike.
        """
        with self._lock:
            needs_fetch = not self._chain_snapshot_loaded

        if needs_fetch:
            chain = self.client.get_option_chain_snapshot(self.symbol)
            with self._lock:
                if not self._chain_snapshot_loaded:
                    added = 0
                    for snap in chain:
                        if snap.ticker not in self._snapshot_cache:
                            self._snapshot_cache[snap.ticker] = snap
                            added += 1
                    evicted = 0
                    while len(self._snapshot_cache) > _GATEWAY_SNAPSHOT_CACHE_MAX:
                        self._snapshot_cache.popitem(last=False)
                        evicted += 1
                    self._chain_snapshot_loaded = True
                    self._track_add(max(0, added - evicted))

        lookup: dict[tuple[float, date], float] = {}
        for contract in contracts:
            with self._lock:
                snap = self._snapshot_cache.get(contract.ticker)
                if snap is not None:
                    self._snapshot_cache.move_to_end(contract.ticker)
            if snap is not None and snap.greeks is not None and snap.greeks.delta is not None:
                lookup[(contract.strike_price, contract.expiration_date)] = snap.greeks.delta
        return lookup


class MarketDataService:
    _MAX_BARS_CACHE_SIZE = 500
    _MAX_BARS_ERRORS_SIZE = 10_000
    _BARS_ERROR_TTL = 60

    def __init__(self, client: MassiveClient) -> None:
        self.client = client
        self._bars_cache: OrderedDict[tuple[str, date, date], _BarsFetchResult] = OrderedDict()
        self._bars_cache_lock = threading.Lock()
        self._bars_inflight: dict[tuple[str, date, date], threading.Event] = {}
        self._bars_errors: dict[tuple[str, date, date], tuple[Exception, float]] = {}
        self._ex_dividend_cache: OrderedDict[tuple[str, date, date], ExDividendLoadResult] = OrderedDict()
        self._ex_dividend_cache_lock = threading.Lock()
        self._redis_cache: OptionDataRedisCache | None = self._build_redis_cache()
        self._contract_catalog: OptionContractCatalogStore | None = self._build_contract_catalog()
        self._historical_contract_catalog: OptionContractCatalogStore | None = self._build_historical_contract_catalog()
        self._historical_store: HistoricalMarketDataStore | None = self._build_historical_store()

    def close(self) -> None:
        """Release Redis cache resources."""
        if hasattr(self, '_redis_cache') and self._redis_cache is not None:
            with contextlib.suppress(Exception):
                self._redis_cache.close()

    @staticmethod
    def _build_redis_cache() -> OptionDataRedisCache | None:
        from backtestforecast.config import get_settings
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache

        settings = get_settings()
        if not settings.option_cache_enabled:
            return None
        redis_url = settings.redis_cache_url
        if not redis_url:
            return None
        try:
            cache = OptionDataRedisCache(redis_url, ttl_seconds=settings.option_cache_ttl_seconds)
            if not cache.ping():
                with contextlib.suppress(Exception):
                    cache.close()
                logger.warning(
                    "market_data.redis_cache_unavailable",
                    redis_url=redis_url.split("@")[-1] if "@" in redis_url else redis_url,
                )
                return None
            return cache
        except Exception:
            logger.warning("market_data.redis_cache_init_failed", exc_info=True)
            return None

    @staticmethod
    def _build_contract_catalog() -> OptionContractCatalogStore | None:
        try:
            return OptionContractCatalogStore(
                session_factory=create_session,
                readonly_session_factory=create_readonly_session,
            )
        except Exception:
            logger.warning("market_data.contract_catalog_init_failed", exc_info=True)
            return None

    @staticmethod
    def _build_historical_contract_catalog() -> OptionContractCatalogStore | None:
        try:
            return OptionContractCatalogStore(
                session_factory=create_session,
                readonly_session_factory=create_readonly_session,
                snapshot_model=HistoricalOptionContractCatalogSnapshot,
            )
        except Exception:
            logger.warning("market_data.historical_contract_catalog_init_failed", exc_info=True)
            return None

    @staticmethod
    def _build_historical_store() -> HistoricalMarketDataStore | None:
        try:
            return HistoricalMarketDataStore(
                session_factory=create_session,
                readonly_session_factory=create_readonly_session,
            )
        except Exception:
            logger.warning("market_data.historical_store_init_failed", exc_info=True)
            return None

    @staticmethod
    def _local_availability_cutoff() -> date:
        return market_date_today() - timedelta(days=1)

    def _prefer_local_history(self, end_date: date) -> bool:
        from backtestforecast.config import get_settings

        settings = get_settings()
        if not settings.historical_data_local_preferred or self._historical_store is None:
            return False
        if not settings.historical_data_t_minus_one_only:
            return True
        return end_date <= self._local_availability_cutoff()

    def _fetch_bars_coalesced(self, symbol: str, start: date, end: date) -> list[DailyBar]:
        return self._fetch_bars_with_metadata(symbol, start, end).bars

    @staticmethod
    def _bars_cover_trading_days(bars: list[DailyBar], start: date, end: date) -> bool:
        if not bars:
            return False
        trade_dates = {bar.trade_date for bar in bars}
        current = start
        while current <= end:
            if is_trading_day(current) and current not in trade_dates:
                return False
            current += timedelta(days=1)
        return True

    def _fetch_bars_with_metadata(self, symbol: str, start: date, end: date) -> _BarsFetchResult:
        """Fetch bars with request coalescing: only one thread fetches per key."""
        cache_key = (symbol, start, end)

        with self._bars_cache_lock:
            cached = self._bars_cache.get(cache_key)
            if cached is not None:
                self._bars_cache.move_to_end(cache_key)
                return cached

            inflight_event = self._bars_inflight.get(cache_key)
            if inflight_event is None:
                inflight_event = threading.Event()
                self._bars_inflight[cache_key] = inflight_event
                am_fetcher = True
            else:
                am_fetcher = False

            if self._prefer_local_history(end):
                local_cached = self._historical_store.get_underlying_day_bars(symbol, start, end) if self._historical_store is not None else []
                local_history_complete = bool(
                    local_cached and self._bars_cover_trading_days(local_cached, local_cached[0].trade_date, end)
                )
                if (
                    local_history_complete
                    # Historical backtests often request a padded warmup window
                    # that begins before the first locally loaded trade date.
                    # Accept the local cache as long as coverage is complete from
                    # the first available local bar onward; prepare_backtest()
                    # separately enforces that enough pre-start bars exist.
                ):
                    if len(self._bars_cache) >= self._MAX_BARS_CACHE_SIZE:
                        self._bars_cache.popitem(last=False)
                    self._bars_cache[cache_key] = _BarsFetchResult(
                        bars=local_cached,
                        local_history_complete=True,
                        local_history_start=local_cached[0].trade_date,
                    )
                    self._bars_cache.move_to_end(cache_key)
                    return self._bars_cache[cache_key]

        try:
            if not am_fetcher:
                from backtestforecast.config import get_settings as _get_settings
                _s = _get_settings()
                coalesce_timeout = _s.massive_timeout_seconds * (_s.massive_max_retries + 1) + 30
                inflight_event.wait(timeout=coalesce_timeout)
                with self._bars_cache_lock:
                    cached = self._bars_cache.get(cache_key)
                    if cached is not None:
                        self._bars_cache.move_to_end(cache_key)
                        return cached
                    error_entry = self._bars_errors.get(cache_key)
                if error_entry is not None:
                    cached_exc = error_entry[0]
                    if isinstance(cached_exc, Exception):
                        try:
                            raise type(cached_exc)(str(cached_exc)) from cached_exc
                        except TypeError:
                            raise DataUnavailableError(str(cached_exc)) from cached_exc
                    raise DataUnavailableError(
                        f"Market data fetch failed: {cached_exc}"
                    )
                raise DataUnavailableError(
                    f"Market data fetch for {symbol} timed out or completed without caching results."
                )

            raw_bars = self.client.get_stock_daily_bars(symbol, start, end)
            with self._bars_cache_lock:
                if cache_key not in self._bars_cache:
                    if len(self._bars_cache) >= self._MAX_BARS_CACHE_SIZE:
                        self._bars_cache.popitem(last=False)
                    self._bars_cache[cache_key] = _BarsFetchResult(bars=raw_bars)
                self._bars_cache.move_to_end(cache_key)
                return self._bars_cache[cache_key]
        except Exception as exc:
            if am_fetcher:
                with self._bars_cache_lock:
                    self._bars_errors[cache_key] = (exc, time.monotonic())
                    now = time.monotonic()
                    stale_keys = [k for k, (_, t) in self._bars_errors.items()
                                  if now - t > self._BARS_ERROR_TTL]
                    for k in stale_keys:
                        self._bars_errors.pop(k, None)
                    if len(self._bars_errors) > self._MAX_BARS_ERRORS_SIZE:
                        by_age = sorted(self._bars_errors.items(), key=lambda kv: kv[1][1])
                        for k, _ in by_age[:len(self._bars_errors) - self._MAX_BARS_ERRORS_SIZE]:
                            self._bars_errors.pop(k, None)
            raise
        finally:
            if am_fetcher:
                inflight_event.set()
                with self._bars_cache_lock:
                    self._bars_inflight.pop(cache_key, None)

    def prepare_backtest(self, request: CreateBacktestRunRequest) -> HistoricalDataBundle:
        total_start = time.perf_counter()
        warmup_trading_days = self._resolve_warmup_trading_days(request)
        extended_start = request.start_date - timedelta(days=(warmup_trading_days * 3))
        extended_end = request.end_date + timedelta(
            days=max(request.max_holding_days, request.target_dte + request.dte_tolerance_days) + 45
        )

        bars_fetch_start = time.perf_counter()
        bars_fetch = self._fetch_bars_with_metadata(request.symbol, extended_start, extended_end)
        bars_fetch_ms = round((time.perf_counter() - bars_fetch_start) * 1000, 3)
        bars_validate_start = time.perf_counter()
        bars = self._validate_bars(bars_fetch.bars, request.symbol)
        bars_validate_ms = round((time.perf_counter() - bars_validate_start) * 1000, 3)

        if not bars:
            raise DataUnavailableError(f"No daily bar data was returned for {request.symbol}.")

        first_entry_index = next(
            (index for index, bar in enumerate(bars) if bar.trade_date >= request.start_date),
            None,
        )
        if first_entry_index is None:
            raise DataUnavailableError(
                f"No tradable daily bars were returned for {request.symbol} in the requested backtest window."
            )
        if first_entry_index < warmup_trading_days:
            raise DataUnavailableError(
                f"Not enough pre-start history was returned to compute indicators for {request.symbol}."
            )

        earnings_start = time.perf_counter()
        earnings_dates = self._load_earnings_dates_if_required(request)
        earnings_ms = round((time.perf_counter() - earnings_start) * 1000, 3)
        data_source = (
            "historical_flatfile"
            if (
                self._prefer_local_history(request.end_date)
                and bars
                and bars_fetch.local_history_complete
                and bars_fetch.local_history_start is not None
                and bars_fetch.local_history_start <= request.start_date
                and bars[0].trade_date <= request.start_date
            )
            else "massive"
        )
        ex_dividend_start = time.perf_counter()
        ex_dividend_result = self._load_ex_dividend_data(
            request.symbol,
            start_date=bars[0].trade_date,
            end_date=bars[-1].trade_date,
        )
        ex_dividend_ms = round((time.perf_counter() - ex_dividend_start) * 1000, 3)
        ex_dividend_dates = ex_dividend_result.dates
        gateway_start = time.perf_counter()
        option_gateway = self.build_option_gateway(
            request.symbol,
            prefer_local=(data_source == "historical_flatfile"),
        )
        option_gateway.set_ex_dividend_dates(ex_dividend_dates)
        gateway_ms = round((time.perf_counter() - gateway_start) * 1000, 3)

        warnings = list(ex_dividend_result.warnings or [])
        if data_source == "historical_flatfile":
            warnings.append(historical_flatfile_pricing_warning())

        bundle = HistoricalDataBundle(
            bars=bars,
            earnings_dates=earnings_dates,
            ex_dividend_dates=ex_dividend_dates,
            option_gateway=option_gateway,
            data_source=data_source,
            warnings=warnings,
        )
        total_ms = round((time.perf_counter() - total_start) * 1000, 3)
        logger.info(
            "market_data.prepare_backtest_timing",
            symbol=request.symbol,
            strategy_type=request.strategy_type.value,
            warmup_trading_days=warmup_trading_days,
            extended_start=extended_start.isoformat(),
            extended_end=extended_end.isoformat(),
            requested_start=request.start_date.isoformat(),
            requested_end=request.end_date.isoformat(),
            bars_count=len(bars),
            data_source=data_source,
            local_history_complete=bars_fetch.local_history_complete,
            local_history_start=bars_fetch.local_history_start.isoformat() if bars_fetch.local_history_start else None,
            earnings_count=len(earnings_dates),
            ex_dividend_count=len(ex_dividend_dates),
            warning_count=len(warnings),
            bars_fetch_ms=bars_fetch_ms,
            bars_validate_ms=bars_validate_ms,
            earnings_ms=earnings_ms,
            ex_dividend_ms=ex_dividend_ms,
            gateway_ms=gateway_ms,
            total_ms=total_ms,
        )
        return bundle

    @staticmethod
    def _collect_avoid_earnings_rules(rule_groups: list[list[Any]]) -> list[AvoidEarningsRule]:
        return [
            rule
            for group in rule_groups
            for rule in group
            if isinstance(rule, AvoidEarningsRule)
        ]

    def load_earnings_dates_for_rules(
        self,
        *,
        symbol: str,
        start_date: date,
        end_date: date,
        rule_groups: list[list[Any]],
    ) -> set[date]:
        avoid_rules = self._collect_avoid_earnings_rules(rule_groups)
        if not avoid_rules:
            return set()

        max_days_before = max(rule.days_before for rule in avoid_rules)
        max_days_after = max(rule.days_after for rule in avoid_rules)
        # Widen the earnings window beyond the backtest range so the engine
        # can evaluate proximity in both directions:
        #   - Subtract max_days_after from start: captures earnings that
        #     occurred *before* the backtest starts, since the engine checks
        #     "are we within N days *after* an earnings event?"
        #   - Add max_days_before to end: captures earnings that occur
        #     *after* the backtest ends, since the engine checks "are we
        #     within N days *before* an earnings event?"
        earnings_start = start_date - timedelta(days=max_days_after)
        earnings_end = end_date + timedelta(days=max_days_before)

        if self._historical_store is not None:
            local = self._historical_store.list_earnings_event_dates(symbol, earnings_start, earnings_end)
            if local:
                return local

        try:
            records = self.client.list_earnings_event_records(symbol, earnings_start, earnings_end)
            dates = {record.event_date for record in records}
            if records and self._historical_store is not None:
                self._historical_store.upsert_earnings_events(
                    [
                        HistoricalEarningsEvent(
                            symbol=symbol,
                            event_date=record.event_date,
                            event_type=record.event_type,
                            provider_event_id=record.provider_event_id,
                            source_file_date=record.event_date,
                        )
                        for record in sorted(records, key=lambda item: (item.event_date, item.event_type, item.provider_event_id or ""))
                    ]
                )
            return dates
        except ExternalServiceError as exc:
            raise DataUnavailableError(
                "The avoid_earnings rule requires an earnings-capable Massive endpoint, "
                "but earnings data could not be retrieved."
            ) from exc

    def _load_earnings_dates_if_required(self, request: CreateBacktestRunRequest) -> set[date]:
        return self.load_earnings_dates_for_rules(
            symbol=request.symbol,
            start_date=request.start_date,
            end_date=request.end_date,
            rule_groups=[request.entry_rules],
        )

    def _load_ex_dividend_dates(
        self,
        symbol: str,
        *,
        start_date: date,
        end_date: date,
    ) -> set[date]:
        return self._load_ex_dividend_data(
            symbol,
            start_date=start_date,
            end_date=end_date,
        ).dates

    def _load_ex_dividend_data(
        self,
        symbol: str,
        *,
        start_date: date,
        end_date: date,
    ) -> ExDividendLoadResult:
        cache_key = (symbol, start_date, end_date)
        cached = self._get_cached_ex_dividend_result(cache_key)
        if cached is not None:
            return cached

        if self._prefer_local_history(end_date) and self._historical_store is not None:
            local = self._historical_store.list_ex_dividend_dates(symbol, start_date, end_date)
            if local:
                result = ExDividendLoadResult(dates=local, warnings=[])
                self._store_ex_dividend_result(cache_key, result)
                if self._redis_cache is not None:
                    self._redis_cache.set_ex_dividend_dates(symbol, start_date, end_date, local)
                return result

        if self._redis_cache is not None:
            from backtestforecast.market_data.redis_cache import CACHE_MISS

            redis_result = self._redis_cache.get_ex_dividend_dates(symbol, start_date, end_date)
            if redis_result is not CACHE_MISS:
                dates, degraded = redis_result
                return ExDividendLoadResult(
                    dates=dates,
                    warnings=self._ex_dividend_warning(symbol, start_date, end_date) if degraded else [],
                )
        try:
            records = self.client.list_ex_dividend_records(symbol, start_date, end_date)
            result = {item.ex_dividend_date for item in records}
            if records and self._historical_store is not None:
                self._historical_store.upsert_ex_dividend_dates(
                    [
                        HistoricalExDividendDate(
                            symbol=symbol,
                            ex_dividend_date=item.ex_dividend_date,
                            provider_dividend_id=item.provider_dividend_id,
                            cash_amount=item.cash_amount,
                            currency=item.currency,
                            declaration_date=item.declaration_date,
                            record_date=item.record_date,
                            pay_date=item.pay_date,
                            frequency=item.frequency,
                            distribution_type=item.distribution_type,
                            historical_adjustment_factor=item.historical_adjustment_factor,
                            split_adjusted_cash_amount=item.split_adjusted_cash_amount,
                            source_file_date=item.ex_dividend_date,
                        )
                        for item in sorted(records, key=lambda record: (record.ex_dividend_date, record.provider_dividend_id or ""))
                    ]
                )
            response = ExDividendLoadResult(dates=result, warnings=[])
            self._store_ex_dividend_result(cache_key, response)
            if self._redis_cache is not None:
                self._redis_cache.set_ex_dividend_dates(symbol, start_date, end_date, result)
            return response
        except ExternalServiceError:
            logger.warning(
                "market_data.ex_dividend_dates_unavailable",
                symbol=symbol,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                exc_info=True,
            )
            settings = get_settings()
            if settings.app_env in {"production", "staging"}:
                raise DataUnavailableError(
                    "Ex-dividend data could not be retrieved for this backtest window. "
                    "Production-like environments fail closed to avoid silently incorrect assignment behavior."
                )
            response = ExDividendLoadResult(
                dates=set(),
                warnings=self._ex_dividend_warning(symbol, start_date, end_date),
            )
            if self._redis_cache is not None:
                from backtestforecast.market_data.redis_cache import _NEGATIVE_CACHE_TTL_SECONDS

                self._redis_cache.set_ex_dividend_dates(
                    symbol,
                    start_date,
                    end_date,
                    set(),
                    degraded=True,
                    ttl_seconds=_NEGATIVE_CACHE_TTL_SECONDS,
                )
            return response

    def build_option_gateway(self, symbol: str, *, prefer_local: bool = False) -> MassiveOptionGateway | HistoricalOptionGateway:
        if prefer_local and self._historical_store is not None:
            return HistoricalOptionGateway(
                self._historical_store,
                symbol,
                redis_cache=self._redis_cache,
                contract_catalog=self._historical_contract_catalog,
            )
        return MassiveOptionGateway(
            self.client,
            symbol,
            redis_cache=self._redis_cache,
            contract_catalog=self._contract_catalog,
        )

    def _get_cached_ex_dividend_result(
        self,
        cache_key: tuple[str, date, date],
    ) -> ExDividendLoadResult | None:
        with self._ex_dividend_cache_lock:
            cached = self._ex_dividend_cache.get(cache_key)
            if cached is None:
                return None
            self._ex_dividend_cache.move_to_end(cache_key)
            return ExDividendLoadResult(
                dates=set(cached.dates),
                warnings=list(cached.warnings or []),
            )

    def _store_ex_dividend_result(
        self,
        cache_key: tuple[str, date, date],
        result: ExDividendLoadResult,
    ) -> None:
        with self._ex_dividend_cache_lock:
            self._ex_dividend_cache[cache_key] = ExDividendLoadResult(
                dates=set(result.dates),
                warnings=list(result.warnings or []),
            )
            self._ex_dividend_cache.move_to_end(cache_key)
            while len(self._ex_dividend_cache) > _EX_DIVIDEND_CACHE_MAX:
                self._ex_dividend_cache.popitem(last=False)

    @staticmethod
    def _ex_dividend_warning(
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        return [
            make_warning(
                "ex_dividend_dates_unavailable",
                (
                    f"Ex-dividend dates for {symbol} were unavailable for "
                    f"{start_date.isoformat()} through {end_date.isoformat()}. "
                    "Early-assignment logic may be incomplete for dividend-sensitive strategies."
                ),
                metadata={
                    "symbol": symbol,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )
        ]

    @staticmethod
    def _validate_bars(raw_bars: list[DailyBar], symbol: str) -> list[DailyBar]:
        seen_dates: dict[date, DailyBar] = {}
        dropped = 0
        duplicate_count = 0
        for bar in raw_bars:
            if not (
                math.isfinite(bar.open_price)
                and math.isfinite(bar.high_price)
                and math.isfinite(bar.low_price)
                and math.isfinite(bar.close_price)
                and math.isfinite(bar.volume)
            ):
                dropped += 1
                continue
            if (
                bar.close_price <= 0
                or bar.open_price <= 0
                # Zero-volume bars are dropped as they often indicate data corrections
                # or partial trading days. This can create gaps in the bar series that
                # affect indicator calculations (e.g., SMA window becomes shorter).
                # See indicators/calculations.py MACD fix for gap-aware handling.
                or bar.volume <= 0
                or bar.high_price < bar.low_price
                or bar.high_price < max(bar.open_price, bar.close_price)
                or bar.low_price > min(bar.open_price, bar.close_price)
            ):
                dropped += 1
                continue
            if bar.trade_date in seen_dates:
                duplicate_count += 1
                existing = seen_dates[bar.trade_date]
                if existing.close_price != bar.close_price:
                    logger.warning(
                        "market_data.duplicate_date_price_discrepancy",
                        symbol=symbol,
                        trade_date=str(bar.trade_date),
                        existing_close=existing.close_price,
                        new_close=bar.close_price,
                    )
            seen_dates[bar.trade_date] = bar
        if dropped:
            logger.warning("market_data.bars_filtered", symbol=symbol, dropped=dropped)
        if duplicate_count:
            logger.warning("market_data.duplicate_dates", symbol=symbol, count=duplicate_count)
        return sorted(seen_dates.values(), key=lambda b: b.trade_date)

    @staticmethod
    def _resolve_warmup_trading_days(request: CreateBacktestRunRequest) -> int:
        warmup = 0
        for rule in request.entry_rules:
            if isinstance(rule, RsiRule):
                warmup = max(warmup, rule.period + 1)
            elif isinstance(rule, MovingAverageCrossoverRule):
                warmup = max(warmup, rule.slow_period + 2)
            elif isinstance(rule, MacdRule):
                warmup = max(warmup, rule.slow_period + rule.signal_period + 2)
            elif isinstance(rule, BollingerBandsRule):
                warmup = max(warmup, rule.period + 2)
            elif isinstance(rule, (IvRankRule, IvPercentileRule)):
                warmup = max(warmup, rule.lookback_days + 5)
            elif isinstance(rule, (VolumeSpikeRule, SupportResistanceRule)):
                warmup = max(warmup, rule.lookback_period + 2)
            elif isinstance(rule, RegimeRule):
                warmup = max(warmup, REGIME_MIN_BARS)
            elif isinstance(rule, IndicatorThresholdRule):
                warmup = max(warmup, MarketDataService._indicator_series_warmup(rule.series))
            elif isinstance(rule, IndicatorTrendRule):
                warmup = max(warmup, MarketDataService._indicator_series_warmup(rule.series) + rule.bars - 1)
            elif isinstance(rule, IndicatorLevelCrossRule):
                warmup = max(warmup, MarketDataService._indicator_series_warmup(rule.series) + 1)
            elif isinstance(rule, IndicatorSeriesCrossRule):
                warmup = max(
                    warmup,
                    max(
                        MarketDataService._indicator_series_warmup(rule.left_series),
                        MarketDataService._indicator_series_warmup(rule.right_series),
                    ) + 1,
                )
            elif isinstance(rule, IndicatorPersistenceRule):
                warmup = max(warmup, MarketDataService._indicator_series_warmup(rule.series) + rule.bars - 1)
        return max(warmup, 2)

    @staticmethod
    def _indicator_series_warmup(series: IndicatorSeries) -> int:
        if isinstance(series, CloseSeries):
            return 2
        if isinstance(series, RsiSeriesSpec):
            return series.period + 1
        if isinstance(series, (SmaSeries, EmaSeries)):
            return series.period + 1
        if isinstance(series, (MacdLineSeries, MacdSignalSeries, MacdHistogramSeries)):
            return series.slow_period + series.signal_period + 2
        if isinstance(series, BollingerBandSeries):
            return series.period + 2
        if isinstance(series, (IvRankSeries, IvPercentileSeries)):
            return series.lookback_days + 5
        if isinstance(series, VolumeRatioSeries):
            return series.lookback_period + 1
        if isinstance(series, (CciSeries, MfiSeries, WilliamsRSeries, AdxSeries)):
            if isinstance(series, AdxSeries):
                return (series.period * 2) + 1
            return series.period + 1
        if isinstance(series, RocSeries):
            return series.period + 1
        if isinstance(series, (StochasticKSeries, StochasticDSeries)):
            return series.k_period + series.smooth_k + series.d_period
        return 2
