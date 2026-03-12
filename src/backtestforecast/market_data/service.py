from __future__ import annotations

import math
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, timedelta

import structlog

from backtestforecast.errors import DataUnavailableError, ExternalServiceError, ValidationError
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


class MassiveOptionGateway:
    def __init__(self, client: MassiveClient, symbol: str) -> None:
        self.client = client
        self.symbol = symbol
        self._contract_cache: OrderedDict[tuple[date, str, int, int], list[OptionContractRecord]] = OrderedDict()
        self._quote_cache: OrderedDict[tuple[str, date], OptionQuoteRecord | None] = OrderedDict()
        self._snapshot_cache: OrderedDict[str, OptionSnapshotRecord | None] = OrderedDict()
        self._chain_snapshot_loaded: bool = False
        self._lock = threading.Lock()

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
        contracts = self.client.list_option_contracts(
            symbol=self.symbol,
            as_of_date=entry_date,
            contract_type=contract_type,
            expiration_gte=expiration_gte,
            expiration_lte=expiration_lte,
        )
        contracts = [contract for contract in contracts if contract.shares_per_contract == 100]
        with self._lock:
            if len(self._contract_cache) >= _GATEWAY_CONTRACT_CACHE_MAX:
                for _ in range(len(self._contract_cache) // 4):
                    self._contract_cache.popitem(last=False)
            self._contract_cache[cache_key] = contracts
        return contracts

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
        quote = self.client.get_option_quote_for_date(option_ticker, trade_date)
        with self._lock:
            if len(self._quote_cache) >= _GATEWAY_QUOTE_CACHE_MAX:
                for _ in range(len(self._quote_cache) // 4):
                    self._quote_cache.popitem(last=False)
            self._quote_cache[cache_key] = quote
        return quote

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
            if len(self._snapshot_cache) >= _GATEWAY_SNAPSHOT_CACHE_MAX:
                for _ in range(len(self._snapshot_cache) // 4):
                    self._snapshot_cache.popitem(last=False)
            self._snapshot_cache[option_ticker] = snapshot
        return snapshot

    def get_chain_delta_lookup(
        self,
        contracts: list[OptionContractRecord],
    ) -> dict[float, float]:
        """Build a strike->delta lookup using the chain snapshot endpoint.

        Fetches the full option chain snapshot once, caches individual results,
        and returns a mapping of strike_price to absolute delta for the given
        contracts. Strikes without available delta are omitted.
        """
        if not self._chain_snapshot_loaded:
            chain = self.client.get_option_chain_snapshot(self.symbol)
            with self._lock:
                for snap in chain:
                    if snap.ticker not in self._snapshot_cache:
                        self._snapshot_cache[snap.ticker] = snap
                self._chain_snapshot_loaded = True

        lookup: dict[float, float] = {}
        for contract in contracts:
            snap = self._snapshot_cache.get(contract.ticker)
            if snap is not None and snap.greeks is not None and snap.greeks.delta is not None:
                lookup[contract.strike_price] = snap.greeks.delta
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
    def __init__(self, client: MassiveClient) -> None:
        self.client = client

    def prepare_backtest(self, request: CreateBacktestRunRequest) -> HistoricalDataBundle:

        warmup_trading_days = self._resolve_warmup_trading_days(request)
        extended_start = request.start_date - timedelta(days=(warmup_trading_days * 3))
        extended_end = request.end_date + timedelta(
            days=max(request.max_holding_days, request.target_dte + request.dte_tolerance_days) + 45
        )

        raw_bars = self.client.get_stock_daily_bars(request.symbol, extended_start, extended_end)
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
        option_gateway = MassiveOptionGateway(self.client, request.symbol)

        return HistoricalDataBundle(
            bars=bars,
            earnings_dates=earnings_dates,
            option_gateway=option_gateway,
        )

    def prepare_long_option_backtest(self, request: CreateBacktestRunRequest) -> HistoricalDataBundle:
        return self.prepare_backtest(request)

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
                or bar.volume <= 0
                or bar.high_price < bar.low_price
                or bar.high_price < max(bar.open_price, bar.close_price)
                or bar.low_price > min(bar.open_price, bar.close_price)
            ):
                dropped += 1
                continue
            seen_dates[bar.trade_date] = bar
        if dropped:
            logger.warning("market_data.bars_filtered", symbol=symbol, dropped=dropped)
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
