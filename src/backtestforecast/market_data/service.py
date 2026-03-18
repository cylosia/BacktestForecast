from __future__ import annotations

import math
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, timedelta

import structlog

from backtestforecast.errors import DataUnavailableError, ExternalServiceError
from backtestforecast.observability.metrics import MARKET_DATA_CACHE_HITS, MARKET_DATA_CACHE_MISSES
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord, OptionSnapshotRecord
from backtestforecast.schemas.backtests import (
    AvoidEarningsRule,
    BollingerBandsRule,
    CreateBacktestRunRequest,
    IvPercentileRule,
    IvRankRule,
    MacdRule,
    MovingAverageCrossoverRule,
    RsiRule,
    SupportResistanceRule,
    VolumeSpikeRule,
)

logger = structlog.get_logger("market_data")


@dataclass(frozen=True, slots=True)
class HistoricalDataBundle:
    bars: list[DailyBar]
    earnings_dates: set[date]
    option_gateway: "MassiveOptionGateway"


_GATEWAY_CONTRACT_CACHE_MAX = 2_000
_GATEWAY_QUOTE_CACHE_MAX = 10_000
_GATEWAY_SNAPSHOT_CACHE_MAX = 5_000
_GATEWAY_IV_CACHE_MAX = 50_000

_global_cache_entries = 0
_global_cache_lock = threading.Lock()
_GLOBAL_CACHE_BUDGET = 50_000


def get_global_cache_entries() -> int:
    """Return the approximate number of entries across all gateway caches."""
    return _global_cache_entries


class MassiveOptionGateway:
    def __init__(
        self,
        client: MassiveClient,
        symbol: str,
        redis_cache: "OptionDataRedisCache | None" = None,
    ) -> None:
        self.client = client
        self.symbol = symbol
        self._redis_cache = redis_cache
        self._contract_cache: OrderedDict[tuple[date, str, int, int], list[OptionContractRecord]] = OrderedDict()
        self._quote_cache: OrderedDict[tuple[str, date], OptionQuoteRecord | None] = OrderedDict()
        self._snapshot_cache: OrderedDict[str, OptionSnapshotRecord | None] = OrderedDict()
        self._iv_cache: OrderedDict[tuple[str, date], float | None] = OrderedDict()
        self._chain_snapshot_loaded: bool = False
        self._lock = threading.Lock()
        self._tracked_entries = 0

    def _track_add(self, count: int = 1) -> None:
        global _global_cache_entries
        with _global_cache_lock:
            _global_cache_entries = max(0, _global_cache_entries + count)
        self._tracked_entries = max(0, self._tracked_entries + count)

    def _track_remove(self, count: int = 1) -> None:
        global _global_cache_entries
        with _global_cache_lock:
            _global_cache_entries = max(0, _global_cache_entries - count)
        self._tracked_entries = max(0, self._tracked_entries - count)

    def _is_over_budget(self) -> bool:
        return _global_cache_entries > _GLOBAL_CACHE_BUDGET

    def clear_caches(self) -> None:
        """Release all in-memory cache entries for this gateway."""
        with self._lock:
            total = (
                len(self._contract_cache) + len(self._quote_cache)
                + len(self._snapshot_cache) + len(self._iv_cache)
            )
            self._contract_cache.clear()
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

        expiration_gte = entry_date + timedelta(days=max(1, target_dte - dte_tolerance_days))
        expiration_lte = entry_date + timedelta(days=target_dte + dte_tolerance_days)

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

        if self._redis_cache is not None:
            self._redis_cache.set_contracts(
                self.symbol, entry_date, contract_type, expiration_gte, expiration_lte, contracts,
            )

        contracts = [contract for contract in contracts if contract.shares_per_contract == 100]
        self._store_contracts_in_memory(cache_key, contracts)
        return contracts

    def _store_contracts_in_memory(
        self,
        cache_key: tuple[date, str, int, int],
        contracts: list[OptionContractRecord],
    ) -> None:
        with self._lock:
            if len(self._contract_cache) >= _GATEWAY_CONTRACT_CACHE_MAX or self._is_over_budget():
                evicted = 0
                for _ in range(len(self._contract_cache) // 4):
                    self._contract_cache.popitem(last=False)
                    evicted += 1
                self._track_remove(evicted)
            self._contract_cache[cache_key] = contracts
            self._track_add(1)

    def select_contract(
        self,
        entry_date: date,
        strategy_type: str,
        underlying_close: float,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> OptionContractRecord:
        _CALL_STRATEGIES = {
            "long_call", "covered_call", "naked_call",
            "bull_call_debit_spread", "bear_call_credit_spread",
            "poor_mans_covered_call",
        }
        contract_type = "call" if strategy_type in _CALL_STRATEGIES else "put"
        contracts = self.list_contracts(
            entry_date=entry_date,
            contract_type=contract_type,
            target_dte=target_dte,
            dte_tolerance_days=dte_tolerance_days,
        )

        if not contracts:
            raise DataUnavailableError(
                f"No eligible {contract_type} contracts were found for {self.symbol} on {entry_date.isoformat()}."
            )

        chosen_expiration = min(
            {contract.expiration_date for contract in contracts},
            key=lambda expiration: self._expiration_sort_key(
                expiration=expiration,
                entry_date=entry_date,
                target_dte=target_dte,
            ),
        )

        expiration_candidates = [contract for contract in contracts if contract.expiration_date == chosen_expiration]
        return min(
            expiration_candidates,
            key=lambda contract: self._strike_sort_key(
                strategy_type=strategy_type,
                strike_price=contract.strike_price,
                underlying_close=underlying_close,
            ),
        )

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None:
        cache_key = (option_ticker, trade_date)
        with self._lock:
            if cache_key in self._quote_cache:
                self._quote_cache.move_to_end(cache_key)
                return self._quote_cache[cache_key]

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
            self._redis_cache.set_quote(option_ticker, trade_date, quote)

        self._store_quote_in_memory(cache_key, quote)
        return quote

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
            if not self._chain_snapshot_loaded:
                chain = self.client.get_option_chain_snapshot(self.symbol)
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

    @staticmethod
    def _expiration_sort_key(expiration: date, entry_date: date, target_dte: int) -> tuple[int, int, int]:
        dte = (expiration - entry_date).days
        return (abs(dte - target_dte), 0 if dte >= target_dte else 1, dte)

    @staticmethod
    def _strike_sort_key(strategy_type: str, strike_price: float, underlying_close: float) -> tuple[float, int, float]:
        distance = abs(strike_price - underlying_close)
        if strategy_type in {"long_call", "covered_call"}:
            tie_bias = 0 if strike_price >= underlying_close else 1
        else:
            tie_bias = 0 if strike_price <= underlying_close else 1
        return (distance, tie_bias, strike_price)


class MarketDataService:
    _MAX_BARS_CACHE_SIZE = 500
    _MAX_BARS_ERRORS_SIZE = 10_000
    _BARS_ERROR_TTL = 60

    def __init__(self, client: MassiveClient) -> None:
        self.client = client
        self._bars_cache: OrderedDict[tuple[str, date, date], list[DailyBar]] = OrderedDict()
        self._bars_cache_lock = threading.Lock()
        self._bars_inflight: dict[tuple[str, date, date], threading.Event] = {}
        self._bars_errors: dict[tuple[str, date, date], tuple[Exception, float]] = {}
        self._redis_cache: OptionDataRedisCache | None = self._build_redis_cache()

    def close(self) -> None:
        """Release Redis cache resources."""
        if hasattr(self, '_redis_cache') and self._redis_cache is not None:
            try:
                self._redis_cache.close()
            except Exception:
                pass

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
            return OptionDataRedisCache(redis_url, ttl_seconds=settings.option_cache_ttl_seconds)
        except Exception:
            logger.warning("market_data.redis_cache_init_failed", exc_info=True)
            return None

    def _fetch_bars_coalesced(self, symbol: str, start: date, end: date) -> list[DailyBar]:
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

        try:
            if not am_fetcher:
                inflight_event.wait(timeout=600)
                with self._bars_cache_lock:
                    cached = self._bars_cache.get(cache_key)
                    if cached is not None:
                        self._bars_cache.move_to_end(cache_key)
                        return cached
                    error_entry = self._bars_errors.get(cache_key)
                if error_entry is not None:
                    cached_exc = error_entry[0]
                    raise DataUnavailableError(
                        f"Market data fetch failed: {cached_exc}"
                    ) from cached_exc
                raise DataUnavailableError(
                    f"Market data fetch for {symbol} timed out or completed without caching results."
                )

            raw_bars = self.client.get_stock_daily_bars(symbol, start, end)
            with self._bars_cache_lock:
                if cache_key not in self._bars_cache:
                    if len(self._bars_cache) >= self._MAX_BARS_CACHE_SIZE:
                        self._bars_cache.popitem(last=False)
                    self._bars_cache[cache_key] = raw_bars
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

        warmup_trading_days = self._resolve_warmup_trading_days(request)
        extended_start = request.start_date - timedelta(days=(warmup_trading_days * 3))
        extended_end = request.end_date + timedelta(
            days=max(request.max_holding_days, request.target_dte + request.dte_tolerance_days) + 45
        )

        raw_bars = self._fetch_bars_coalesced(request.symbol, extended_start, extended_end)
        bars = self._validate_bars(raw_bars, request.symbol)

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

        earnings_dates = self._load_earnings_dates_if_required(request)
        option_gateway = MassiveOptionGateway(
            self.client, request.symbol, redis_cache=self._redis_cache,
        )

        return HistoricalDataBundle(
            bars=bars,
            earnings_dates=earnings_dates,
            option_gateway=option_gateway,
        )

    def _load_earnings_dates_if_required(self, request: CreateBacktestRunRequest) -> set[date]:
        avoid_rules = [rule for rule in request.entry_rules if isinstance(rule, AvoidEarningsRule)]
        if not avoid_rules:
            return set()

        max_days_before = max(rule.days_before for rule in avoid_rules)
        max_days_after = max(rule.days_after for rule in avoid_rules)
        earnings_start = request.start_date - timedelta(days=max_days_after)
        earnings_end = request.end_date + timedelta(days=max_days_before)

        try:
            return self.client.list_earnings_event_dates(request.symbol, earnings_start, earnings_end)
        except ExternalServiceError as exc:
            raise DataUnavailableError(
                "The avoid_earnings rule requires an earnings-capable Massive endpoint, "
                "but earnings data could not be retrieved."
            ) from exc

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
            elif isinstance(rule, VolumeSpikeRule):
                warmup = max(warmup, rule.lookback_period + 2)
            elif isinstance(rule, SupportResistanceRule):
                warmup = max(warmup, rule.lookback_period + 2)
        return max(warmup, 2)
