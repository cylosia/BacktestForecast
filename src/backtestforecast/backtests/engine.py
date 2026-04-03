from __future__ import annotations

import inspect
import math
import time as _time
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import structlog

from backtestforecast.backtests.rules import EntryRuleComputationCache, EntryRuleEvaluator
from backtestforecast.backtests.strategies.common import (
    BuildPositionProfiler,
    activate_build_position_profiler,
    current_build_position_phase,
    reset_build_position_profiler,
)
from backtestforecast.backtests.strategies.registry import STRATEGY_REGISTRY
from backtestforecast.backtests.strategies.wheel import WheelStrategyBacktestEngine
from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import (
    DEFAULT_CONTRACT_MULTIPLIER,
    BacktestConfig,
    BacktestExecutionResult,
    EquityPointResult,
    OpenMultiLegPosition,
    OpenOptionLeg,
    OpenStockLeg,
    OptionDataGateway,
    PositionSnapshot,
    TradeResult,
)
from backtestforecast.errors import AppValidationError, DataUnavailableError
from backtestforecast.market_data.types import DailyBar, OptionQuoteRecord

logger = structlog.get_logger(__name__)

CONTRACT_MULTIPLIER = DEFAULT_CONTRACT_MULTIPLIER


def _leg_multiplier(leg: object) -> float:
    """Return the contract multiplier for a leg, defaulting to 100."""
    return getattr(leg, "contract_multiplier", CONTRACT_MULTIPLIER)


_D0 = Decimal("0")
_D100 = Decimal("100")
_D365 = Decimal("365")
_D_FIVE_CENTS = Decimal("0.05")


_D_CACHE: dict[int | float, Decimal] = {
    0: _D0, 1: Decimal("1"), -1: Decimal("-1"), 100: _D100,
}

_D_CACHE_MAX = 4096
_OPTION_SIGNED_UNIT_FACTOR_CACHE: dict[tuple[int, int, float], Decimal] = {}
_OPTION_ABS_UNIT_FACTOR_CACHE: dict[tuple[int, float], Decimal] = {}
_STOCK_SIGNED_UNIT_FACTOR_CACHE: dict[tuple[int, int], Decimal] = {}
_STOCK_ABS_UNIT_FACTOR_CACHE: dict[int, Decimal] = {}


def _D(v: float | int) -> Decimal:
    """Convert a float/int to Decimal via string to avoid IEEE 754 artefacts.

    Results are cached (up to 4096 entries) because the same prices recur
    across bars during mark-to-market and commission calculations.
    """
    cached = _D_CACHE.get(v)
    if cached is not None:
        return cached
    result = Decimal(str(v))
    if len(_D_CACHE) < _D_CACHE_MAX:
        _D_CACHE[v] = result
    return result


def _option_signed_unit_factor(leg: OpenOptionLeg | Any) -> Decimal:
    key = (leg.side, leg.quantity_per_unit, _leg_multiplier(leg))
    cached = _OPTION_SIGNED_UNIT_FACTOR_CACHE.get(key)
    if cached is not None:
        return cached
    factor = _D(leg.side) * _D(leg.quantity_per_unit) * _D(_leg_multiplier(leg))
    _OPTION_SIGNED_UNIT_FACTOR_CACHE[key] = factor
    return factor


def _option_abs_unit_factor(leg: OpenOptionLeg | Any) -> Decimal:
    key = (leg.quantity_per_unit, _leg_multiplier(leg))
    cached = _OPTION_ABS_UNIT_FACTOR_CACHE.get(key)
    if cached is not None:
        return cached
    factor = _D(leg.quantity_per_unit) * _D(_leg_multiplier(leg))
    _OPTION_ABS_UNIT_FACTOR_CACHE[key] = factor
    return factor


def _stock_signed_unit_factor(leg: OpenStockLeg | Any) -> Decimal:
    key = (leg.side, leg.share_quantity_per_unit)
    cached = _STOCK_SIGNED_UNIT_FACTOR_CACHE.get(key)
    if cached is not None:
        return cached
    factor = _D(leg.side) * _D(leg.share_quantity_per_unit)
    _STOCK_SIGNED_UNIT_FACTOR_CACHE[key] = factor
    return factor


def _stock_abs_unit_factor(leg: OpenStockLeg | Any) -> Decimal:
    key = leg.share_quantity_per_unit
    cached = _STOCK_ABS_UNIT_FACTOR_CACHE.get(key)
    if cached is not None:
        return cached
    factor = _D(leg.share_quantity_per_unit)
    _STOCK_ABS_UNIT_FACTOR_CACHE[key] = factor
    return factor


@dataclass(slots=True)
class _ExitPolicyLane:
    config: BacktestConfig
    cash: Decimal
    peak_equity: Decimal
    position: OpenMultiLegPosition | None = None
    trades: list[TradeResult] = field(default_factory=list)
    equity_curve: list[EquityPointResult] = field(default_factory=list)
    warnings: list[dict[str, Any]] = field(default_factory=list)
    warning_codes: set[str] = field(default_factory=set)


@dataclass(slots=True)
class _EnginePhaseTiming:
    rule_precompute_ms: float = 0.0
    mark_position_ms: float = 0.0
    exit_resolution_ms: float = 0.0
    build_position_ms: float = 0.0
    build_contract_fetch_ms: float = 0.0
    build_contract_selector_fetch_ms: float = 0.0
    build_contract_availability_fetch_ms: float = 0.0
    build_contract_batch_fetch_ms: float = 0.0
    build_contract_exact_fetch_ms: float = 0.0
    build_contract_other_ms: float = 0.0
    build_contract_selection_cache_hits: int = 0
    build_contract_selection_cache_misses: int = 0
    build_contract_gateway_method_ms: dict[str, float] = field(default_factory=dict)
    build_contract_gateway_method_calls: dict[str, int] = field(default_factory=dict)
    build_contract_gateway_contract_cache_hits: int = 0
    build_contract_gateway_contract_cache_misses: int = 0
    build_contract_gateway_exact_cache_hits: int = 0
    build_contract_gateway_exact_cache_misses: int = 0
    build_contract_gateway_availability_cache_hits: int = 0
    build_contract_gateway_availability_cache_misses: int = 0
    build_contract_gateway_availability_by_type_cache_hits: int = 0
    build_contract_gateway_availability_by_type_cache_misses: int = 0
    build_delta_resolution_ms: float = 0.0
    build_delta_iv_quote_fetch_ms: float = 0.0
    build_delta_iv_solve_ms: float = 0.0
    build_delta_kernel_ms: float = 0.0
    build_delta_other_ms: float = 0.0
    build_delta_lookup_cache_hits: int = 0
    build_delta_lookup_cache_misses: int = 0
    build_delta_iv_cache_hits: int = 0
    build_delta_iv_cache_misses: int = 0
    build_entry_quote_fetch_ms: float = 0.0
    build_object_construction_ms: float = 0.0
    attach_quote_series_ms: float = 0.0
    position_sizing_ms: float = 0.0
    current_position_value_ms: float = 0.0
    close_position_ms: float = 0.0
    equity_curve_ms: float = 0.0
    force_close_ms: float = 0.0
    summary_ms: float = 0.0
    total_ms: float = 0.0
    bars_processed: int = 0
    bars_skipped_before_start: int = 0
    positions_opened: int = 0
    positions_closed: int = 0
    force_closes: int = 0


def _elapsed_ms(start: float) -> float:
    return (_time.perf_counter() - start) * 1000.0


class _TimedBuildPositionGateway:
    _CONTRACT_FETCH_METHODS = {
        "list_contracts",
        "list_contracts_for_preferred_expiration",
        "list_contracts_for_preferred_common_expiration",
        "list_contracts_for_expiration",
        "list_contracts_for_expirations",
        "list_contracts_for_expirations_by_type",
        "list_available_expirations",
        "list_available_expirations_by_type",
    }
    _ENTRY_QUOTE_METHODS = {"get_quote", "get_quotes"}

    def __init__(self, gateway: OptionDataGateway, profiler: BuildPositionProfiler) -> None:
        self._gateway = gateway
        self._profiler = profiler

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._gateway, name)
        if not callable(attr):
            return attr
        if name not in self._CONTRACT_FETCH_METHODS and name not in self._ENTRY_QUOTE_METHODS:
            return attr

        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            start = _time.perf_counter()
            try:
                return attr(*args, **kwargs)
            finally:
                elapsed_ms = _elapsed_ms(start)
                phase = current_build_position_phase()
                if name in self._CONTRACT_FETCH_METHODS:
                    self._profiler.contract_gateway_method_ms[name] = (
                        self._profiler.contract_gateway_method_ms.get(name, 0.0) + elapsed_ms
                    )
                    self._profiler.contract_gateway_method_calls[name] = (
                        self._profiler.contract_gateway_method_calls.get(name, 0) + 1
                    )
                if phase in {"contract_fetch", "delta_lookup"}:
                    pass
                elif name in self._CONTRACT_FETCH_METHODS:
                    self._profiler.contract_fetch_ms += elapsed_ms
                elif name in self._ENTRY_QUOTE_METHODS:
                    self._profiler.entry_quote_fetch_ms += elapsed_ms

        return _wrapped


class OptionsBacktestEngine:
    def __init__(self) -> None:
        self._wheel_engine: WheelStrategyBacktestEngine | None = None

    @property
    def wheel_engine(self) -> WheelStrategyBacktestEngine:
        if self._wheel_engine is None:
            self._wheel_engine = WheelStrategyBacktestEngine()
        return self._wheel_engine

    @staticmethod
    def _can_afford_minimum_strategy_package(
        strategy: object,
        config: BacktestConfig,
        bar: DailyBar,
        available_cash: Decimal,
    ) -> bool:
        estimator = getattr(strategy, "estimate_minimum_capital_required_per_unit", None)
        if estimator is None:
            return True
        try:
            minimum_capital = estimator(config, bar)
        except Exception:
            logger.warning(
                "engine.minimum_capital_estimate_failed",
                strategy=getattr(strategy, "strategy_type", None),
                exc_info=True,
            )
            return True
        if minimum_capital is None:
            return True
        quantity = OptionsBacktestEngine._resolve_position_size(
            available_cash=available_cash,
            account_size=float(config.account_size),
            risk_per_trade_pct=float(config.risk_per_trade_pct),
            capital_required_per_unit=minimum_capital,
            max_loss_per_unit=minimum_capital,
            entry_cost_per_unit=minimum_capital,
            commission_per_unit=float(config.commission_per_contract),
            slippage_pct=config.slippage_pct,
            gross_notional_per_unit=minimum_capital,
        )
        return quantity > 0

    @staticmethod
    def _record_build_position_subtiming(
        timing: _EnginePhaseTiming,
        *,
        total_ms: float,
        profiler: BuildPositionProfiler,
    ) -> None:
        timing.build_position_ms += total_ms
        timing.build_contract_fetch_ms += profiler.contract_fetch_ms
        timing.build_contract_selector_fetch_ms += profiler.contract_selector_fetch_ms
        timing.build_contract_availability_fetch_ms += profiler.contract_availability_fetch_ms
        timing.build_contract_batch_fetch_ms += profiler.contract_batch_fetch_ms
        timing.build_contract_exact_fetch_ms += profiler.contract_exact_fetch_ms
        timing.build_contract_selection_cache_hits += profiler.contract_selection_cache_hits
        timing.build_contract_selection_cache_misses += profiler.contract_selection_cache_misses
        timing.build_contract_gateway_contract_cache_hits += profiler.contract_gateway_contract_cache_hits
        timing.build_contract_gateway_contract_cache_misses += profiler.contract_gateway_contract_cache_misses
        timing.build_contract_gateway_exact_cache_hits += profiler.contract_gateway_exact_cache_hits
        timing.build_contract_gateway_exact_cache_misses += profiler.contract_gateway_exact_cache_misses
        timing.build_contract_gateway_availability_cache_hits += profiler.contract_gateway_availability_cache_hits
        timing.build_contract_gateway_availability_cache_misses += profiler.contract_gateway_availability_cache_misses
        timing.build_contract_gateway_availability_by_type_cache_hits += (
            profiler.contract_gateway_availability_by_type_cache_hits
        )
        timing.build_contract_gateway_availability_by_type_cache_misses += (
            profiler.contract_gateway_availability_by_type_cache_misses
        )
        for method_name, elapsed_ms in profiler.contract_gateway_method_ms.items():
            timing.build_contract_gateway_method_ms[method_name] = (
                timing.build_contract_gateway_method_ms.get(method_name, 0.0) + elapsed_ms
            )
        for method_name, call_count in profiler.contract_gateway_method_calls.items():
            timing.build_contract_gateway_method_calls[method_name] = (
                timing.build_contract_gateway_method_calls.get(method_name, 0) + call_count
            )
        contract_residual = (
            profiler.contract_fetch_ms
            - profiler.contract_selector_fetch_ms
            - profiler.contract_availability_fetch_ms
            - profiler.contract_batch_fetch_ms
            - profiler.contract_exact_fetch_ms
        )
        timing.build_contract_other_ms += max(0.0, contract_residual)
        timing.build_delta_resolution_ms += profiler.delta_lookup_ms
        timing.build_delta_iv_quote_fetch_ms += profiler.delta_iv_quote_fetch_ms
        timing.build_delta_iv_solve_ms += profiler.delta_iv_solve_ms
        timing.build_delta_kernel_ms += profiler.delta_kernel_ms
        timing.build_delta_lookup_cache_hits += profiler.delta_lookup_cache_hits
        timing.build_delta_lookup_cache_misses += profiler.delta_lookup_cache_misses
        timing.build_delta_iv_cache_hits += profiler.delta_iv_cache_hits
        timing.build_delta_iv_cache_misses += profiler.delta_iv_cache_misses
        delta_residual = (
            profiler.delta_lookup_ms
            - profiler.delta_iv_quote_fetch_ms
            - profiler.delta_iv_solve_ms
            - profiler.delta_kernel_ms
        )
        timing.build_delta_other_ms += max(0.0, delta_residual)
        timing.build_entry_quote_fetch_ms += profiler.entry_quote_fetch_ms
        residual = total_ms - profiler.contract_fetch_ms - profiler.delta_lookup_ms - profiler.entry_quote_fetch_ms
        timing.build_object_construction_ms += max(0.0, residual)

    @staticmethod
    def _clone_position_template(position: OpenMultiLegPosition) -> OpenMultiLegPosition:
        return OpenMultiLegPosition(
            display_ticker=position.display_ticker,
            strategy_type=position.strategy_type,
            underlying_symbol=position.underlying_symbol,
            entry_date=position.entry_date,
            entry_index=position.entry_index,
            quantity=position.quantity,
            dte_at_open=position.dte_at_open,
            option_legs=[
                OpenOptionLeg(
                    ticker=leg.ticker,
                    contract_type=leg.contract_type,
                    side=leg.side,
                    strike_price=leg.strike_price,
                    expiration_date=leg.expiration_date,
                    quantity_per_unit=leg.quantity_per_unit,
                    entry_mid=leg.entry_mid,
                    last_mid=leg.last_mid,
                    contract_multiplier=leg.contract_multiplier,
                )
                for leg in position.option_legs
            ],
            stock_legs=[
                OpenStockLeg(
                    symbol=leg.symbol,
                    side=leg.side,
                    share_quantity_per_unit=leg.share_quantity_per_unit,
                    entry_price=leg.entry_price,
                    last_price=leg.last_price,
                )
                for leg in position.stock_legs
            ],
            scheduled_exit_date=position.scheduled_exit_date,
            capital_required_per_unit=position.capital_required_per_unit,
            max_loss_per_unit=position.max_loss_per_unit,
            max_profit_per_unit=position.max_profit_per_unit,
            entry_reason=position.entry_reason,
            entry_commission_total=position.entry_commission_total,
            detail_json=dict(position.detail_json),
            # Quote history is read-mostly and can be safely shared across lanes.
            quote_series_lookup=position.quote_series_lookup,
            quote_series_loaded_tickers=set(position.quote_series_loaded_tickers),
        )

    @staticmethod
    def _build_position_with_timing(
        *,
        strategy: object,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
        custom_legs: list[Any] | None,
        timing: _EnginePhaseTiming,
    ) -> OpenMultiLegPosition | None:
        profiler = BuildPositionProfiler()
        timed_gateway = _TimedBuildPositionGateway(option_gateway, profiler)
        profiler_token = activate_build_position_profiler(profiler)
        build_start = _time.perf_counter()
        try:
            if custom_legs is not None:
                candidate = strategy.build_position(
                    config,
                    bar,
                    bar_index,
                    timed_gateway,
                    custom_legs=custom_legs,
                )
            else:
                candidate = strategy.build_position(
                    config,
                    bar,
                    bar_index,
                    timed_gateway,
                )
        except Exception:
            total_ms = _elapsed_ms(build_start)
            reset_build_position_profiler(profiler_token)
            OptionsBacktestEngine._record_build_position_subtiming(
                timing,
                total_ms=total_ms,
                profiler=profiler,
            )
            raise
        else:
            total_ms = _elapsed_ms(build_start)
            reset_build_position_profiler(profiler_token)
            OptionsBacktestEngine._record_build_position_subtiming(
                timing,
                total_ms=total_ms,
                profiler=profiler,
            )
            return candidate

    def run(
        self,
        config: BacktestConfig,
        bars: list[DailyBar],
        earnings_dates: set[date],
        option_gateway: OptionDataGateway,
        *,
        ex_dividend_dates: set[date] | None = None,
        shared_entry_rule_cache: EntryRuleComputationCache | None = None,
    ) -> BacktestExecutionResult:
        if config.strategy_type == "wheel_strategy":
            return self.wheel_engine.run(
                config=config,
                bars=bars,
                earnings_dates=earnings_dates,
                option_gateway=option_gateway,
                shared_entry_rule_cache=shared_entry_rule_cache,
            )

        strategy = STRATEGY_REGISTRY.get(config.strategy_type)
        if strategy is None:
            raise AppValidationError(f"Unsupported strategy_type: {config.strategy_type}")

        # Decimal precision: Phase 1 & 2 complete.
        #
        # Phase 1: `cash` uses Decimal throughout.
        # Phase 2: `position_value`, `entry_cost`, `gross_pnl`, `net_pnl`,
        # commissions, slippage, and all P&L math use Decimal. TradeResult
        # and EquityPointResult financial fields are Decimal. Float inputs
        # from strategy legs/quotes are converted at the computation boundary
        # via _D(). build_summary converts to float at the statistics boundary.
        #
        # Remaining: OpenOptionLeg/OpenStockLeg price fields are still float
        # (they come from external API data). Conversion happens at the point
        # of financial computation, not at storage.

        if config.start_date > config.end_date:
            raise AppValidationError("start_date must not be after end_date")
        if config.account_size <= 0:
            raise AppValidationError("account_size must be positive")

        timing = _EnginePhaseTiming()
        total_start = _time.perf_counter()
        sorted_bars = sorted(bars, key=lambda bar: bar.trade_date)
        _pre_filter_len = len(sorted_bars)
        sorted_bars = [b for b in sorted_bars if b.close_price > 0]
        if not sorted_bars:
            return BacktestExecutionResult(
                summary=build_summary(
                    float(config.account_size),
                    float(config.account_size),
                    [],
                    [],
                    risk_free_rate=config.risk_free_rate,
                    risk_free_rate_curve=config.risk_free_rate_curve,
                ),
                trades=[], equity_curve=[]
            )
        ex_dividend_dates = set(ex_dividend_dates or ())
        if not ex_dividend_dates and hasattr(option_gateway, "get_ex_dividend_dates"):
            try:
                ex_dividend_dates = option_gateway.get_ex_dividend_dates(
                    sorted_bars[0].trade_date, sorted_bars[-1].trade_date,
                )
            except Exception:
                logger.warning("engine.ex_dividend_dates_unavailable", exc_info=True)

        warnings: list[dict[str, Any]] = []
        warning_codes: set[str] = set()
        if len(sorted_bars) < _pre_filter_len:
            self._add_warning_once(
                warnings, warning_codes, "non_positive_close_filtered",
                f"{_pre_filter_len - len(sorted_bars)} bar(s) with non-positive close price were excluded.",
            )
        cash = Decimal(str(config.account_size))
        peak_equity = cash
        position: OpenMultiLegPosition | None = None
        trades: list[TradeResult] = []
        equity_curve: list[EquityPointResult] = []
        evaluator = EntryRuleEvaluator(
            config=config,
            bars=sorted_bars,
            earnings_dates=earnings_dates,
            option_gateway=option_gateway,
            shared_cache=shared_entry_rule_cache,
        )
        entry_allowed_mask: list[bool] | None = None
        rule_precompute_start = _time.perf_counter()
        try:
            entry_allowed_mask = evaluator.build_entry_allowed_mask()
        except Exception:
            logger.warning("entry_rule_precompute_error", exc_info=True)
            self._add_warning_once(
                warnings,
                warning_codes,
                "entry_rule_evaluation_error",
                "One or more entry rule evaluations failed and were treated as not-allowed.",
            )
        timing.rule_precompute_ms += _elapsed_ms(rule_precompute_start)
        build_position_supports_custom_legs = (
            config.custom_legs is not None
            and "custom_legs" in inspect.signature(strategy.build_position).parameters
        )
        custom_legs = list(config.custom_legs) if build_position_supports_custom_legs else None
        last_bar_date = sorted_bars[-1].trade_date

        for index, bar in enumerate(sorted_bars):
            if bar.trade_date < config.start_date:
                timing.bars_skipped_before_start += 1
                continue
            timing.bars_processed += 1

            position_value = _D0
            exit_prices: dict[str, float] = {}

            if position is not None:
                mark_start = _time.perf_counter()
                snapshot = self._mark_position(
                    position, bar, option_gateway, warnings, warning_codes, ex_dividend_dates,
                )
                timing.mark_position_ms += _elapsed_ms(mark_start)
                position_value = snapshot.position_value
                exit_prices = {leg.ticker: leg.last_mid for leg in position.option_legs}
                for stock_leg in position.stock_legs:
                    exit_prices[stock_leg.symbol] = stock_leg.last_price

                entry_cost = self._entry_value_per_unit(position) * _D(position.quantity)
                if not position_value.is_finite():
                    logger.warning("engine.nan_position_value_exit_guard", bar_date=str(bar.trade_date))
                    position_value = entry_cost
                if not entry_cost.is_finite():
                    logger.warning("engine.nan_entry_cost", bar_date=str(bar.trade_date))
                    entry_cost = _D0
                    position_value = _D0
                if math.isnan(position.capital_required_per_unit):
                    logger.warning(
                        "engine.nan_capital_required_per_unit",
                        ticker=position.display_ticker,
                        bar_date=str(bar.trade_date),
                    )
                    self._add_warning_once(
                        warnings, warning_codes, "nan_capital_required",
                        "Skipped stop/profit check: capital_required_per_unit is NaN.",
                    )
                    capital_at_risk = 0.0
                else:
                    capital_at_risk = position.capital_required_per_unit * position.quantity
                exit_start = _time.perf_counter()
                should_exit, exit_reason = self._resolve_exit(
                    bar=bar,
                    position=position,
                    max_holding_days=config.max_holding_days,
                    backtest_end_date=config.end_date,
                    last_bar_date=last_bar_date,
                    position_value=float(position_value),
                    entry_cost=float(entry_cost),
                    capital_at_risk=capital_at_risk,
                    profit_target_pct=config.profit_target_pct,
                    stop_loss_pct=config.stop_loss_pct,
                    current_bar_index=index,
                )
                timing.exit_resolution_ms += _elapsed_ms(exit_start)
                assignment_detail = snapshot.assignment_detail
                if snapshot.assignment_exit_reason is not None:
                    should_exit = True
                    exit_reason = snapshot.assignment_exit_reason
                if should_exit:
                    close_start = _time.perf_counter()
                    trade, cash_delta = self._close_position(
                        position, config, position_value, bar.trade_date, bar.close_price,
                        exit_prices, exit_reason, warnings, warning_codes,
                        current_bar_index=index,
                        assignment_detail=assignment_detail,
                        trade_warnings=snapshot.warnings,
                    )
                    timing.close_position_ms += _elapsed_ms(close_start)
                    timing.positions_closed += 1
                    cash += cash_delta
                    trades.append(trade)
                    position = None
                    position_value = _D0

            entry_allowed = False
            # Prevent re-entry on the same bar where a position was just closed
            # during *this* iteration. This avoids infinite open/close loops when
            # entry rules remain satisfied after a stop-loss or expiration exit.
            # Note: this blocks all same-day re-entry, which may suppress
            # legitimate signals on high-frequency strategies.
            just_closed_this_bar = (
                position is None
                and len(trades) > 0
                and trades[-1].exit_date == bar.trade_date
            )
            if just_closed_this_bar:
                self._add_warning_once(
                    warnings, warning_codes, "same_day_reentry_blocked",
                    "One or more entry signals were suppressed because a position was "
                    "closed on the same trading day. The engine does not re-enter on "
                    "the same bar to avoid infinite open/close loops.",
                )
            if position is None and not just_closed_this_bar and bar.trade_date <= config.end_date:
                if entry_allowed_mask is not None:
                    entry_allowed = entry_allowed_mask[index]
                else:
                    try:
                        entry_allowed = evaluator.is_entry_allowed(index)
                    except Exception:
                        logger.warning("entry_rule_evaluation_error", bar_index=index, exc_info=True)
                        self._add_warning_once(
                            warnings, warning_codes, "entry_rule_evaluation_error",
                            "One or more entry rule evaluations failed and were treated as not-allowed.",
                        )
            if entry_allowed:
                if not self._can_afford_minimum_strategy_package(strategy, config, bar, cash):
                    self._add_warning_once(
                        warnings,
                        warning_codes,
                        "capital_requirement_exceeded",
                        "One or more signals were skipped because available cash or"
                        " configured risk budget could not support the strategy package.",
                    )
                    entry_allowed = False
            if entry_allowed:
                try:
                    candidate = self._build_position_with_timing(
                        strategy=strategy,
                        config=config,
                        bar=bar,
                        bar_index=index,
                        option_gateway=option_gateway,
                        custom_legs=custom_legs,
                        timing=timing,
                    )
                except DataUnavailableError:
                    self._add_warning_once(
                        warnings,
                        warning_codes,
                        "missing_contract_chain",
                        "One or more entry dates could not be evaluated because no eligible"
                        " option contract chain was returned.",
                        )
                else:
                    if candidate is None:
                        self._add_warning_once(
                            warnings,
                            warning_codes,
                            "missing_entry_quote",
                            "One or more entry dates were skipped because no valid same-day option quote was returned.",
                        )
                    else:
                        ev_per_unit = self._entry_value_per_unit(candidate)
                        contracts_per_unit = sum(leg.quantity_per_unit for leg in candidate.option_legs)
                        commission_per_unit = float(config.commission_per_contract) * contracts_per_unit
                        gross_notional_per_unit = (
                            sum(abs(leg.entry_mid * _leg_multiplier(leg)) * leg.quantity_per_unit for leg in candidate.option_legs)
                            + sum(abs(leg.entry_price * leg.share_quantity_per_unit) for leg in candidate.stock_legs)
                        )
                        open_start = _time.perf_counter()
                        quantity = self._resolve_position_size(
                            available_cash=cash,
                            account_size=float(config.account_size),
                            risk_per_trade_pct=float(config.risk_per_trade_pct),
                            capital_required_per_unit=candidate.capital_required_per_unit,
                            max_loss_per_unit=candidate.max_loss_per_unit,
                            entry_cost_per_unit=float(abs(ev_per_unit)),
                            commission_per_unit=commission_per_unit,
                            slippage_pct=config.slippage_pct,
                            gross_notional_per_unit=gross_notional_per_unit,
                        )
                        if quantity <= 0:
                            timing.position_sizing_ms += _elapsed_ms(open_start)
                            self._add_warning_once(
                                warnings,
                                warning_codes,
                                "capital_requirement_exceeded",
                                "One or more signals were skipped because available cash or"
                                " configured risk budget could not support the strategy package.",
                            )
                        else:
                            candidate.quantity = quantity
                            candidate.detail_json.setdefault("entry_underlying_close", bar.close_price)
                            entry_commission = self._option_commission_total(candidate, config.commission_per_contract)
                            candidate.entry_commission_total = entry_commission
                            slippage_cost_d = _D(gross_notional_per_unit) * _D(quantity) * (_D(config.slippage_pct) / _D100)
                            total_entry_cost = (ev_per_unit * _D(quantity)) + entry_commission + slippage_cost_d
                            timing.position_sizing_ms += _elapsed_ms(open_start)
                            if cash - total_entry_cost < 0:
                                self._add_warning_once(
                                    warnings, warning_codes, "negative_cash_rejected",
                                    "One or more entries were skipped because the total cost "
                                    "(including slippage) would have exceeded available cash.",
                                )
                            else:
                                attach_start = _time.perf_counter()
                                self._attach_position_quote_series(
                                    candidate,
                                    option_gateway=option_gateway,
                                    start_date=bar.trade_date,
                                    end_date=last_bar_date,
                                )
                                timing.attach_quote_series_ms += _elapsed_ms(attach_start)
                                cash -= total_entry_cost
                                position = candidate
                                timing.positions_opened += 1
                                if strategy.margin_warning_message and candidate.capital_required_per_unit > float(
                                    abs(ev_per_unit)
                                ):
                                    self._add_warning_once(
                                        warnings, warning_codes, "margin_reserved", strategy.margin_warning_message
                                    )

            if position is not None:
                current_value_start = _time.perf_counter()
                position_value = self._current_position_value(position, bar.close_price)
                timing.current_position_value_ms += _elapsed_ms(current_value_start)
                if not position_value.is_finite():
                    logger.warning("engine.nan_position_value", ticker=position.display_ticker, bar_date=str(bar.trade_date))
                    position_value = _D0

            equity_start = _time.perf_counter()
            equity = cash + position_value
            if equity < _D0:
                self._add_warning_once(
                    warnings, warning_codes, "negative_equity",
                    "Account equity went negative. This indicates a margin call scenario "
                    "where losses exceeded the account balance. Drawdown percentages above "
                    "100% may occur.",
                )
            peak_equity = max(peak_equity, equity)
            drawdown_pct = (peak_equity - equity) / peak_equity * _D100 if peak_equity > _D0 else _D0
            equity_curve.append(
                EquityPointResult(
                    trade_date=bar.trade_date,
                    equity=equity,
                    cash=cash,
                    position_value=position_value,
                    drawdown_pct=drawdown_pct,
                )
            )
            timing.equity_curve_ms += _elapsed_ms(equity_start)

            if position is None and bar.trade_date > config.end_date:
                break

        if position is not None:
            force_close_start = _time.perf_counter()
            mark_start = _time.perf_counter()
            snapshot = self._mark_position(
                position, sorted_bars[-1], option_gateway, warnings, warning_codes, ex_dividend_dates,
            )
            timing.mark_position_ms += _elapsed_ms(mark_start)
            final_position_value = snapshot.position_value
            if not final_position_value.is_finite():
                logger.warning(
                    "engine.nan_position_value_force_close_guard",
                    bar_date=str(sorted_bars[-1].trade_date),
                )
                final_position_value = self._entry_value_per_unit(position) * _D(position.quantity)
                if not final_position_value.is_finite():
                    final_position_value = _D0
            exit_prices_fc = {leg.ticker: leg.last_mid for leg in position.option_legs}
            for stock_leg in position.stock_legs:
                exit_prices_fc[stock_leg.symbol] = stock_leg.last_price
            close_start = _time.perf_counter()
            trade, cash_delta = self._close_position(
                position, config, final_position_value, sorted_bars[-1].trade_date,
                sorted_bars[-1].close_price, exit_prices_fc, "data_exhausted",
                warnings, warning_codes,
                current_bar_index=len(sorted_bars) - 1,
                trade_warnings=snapshot.warnings,
            )
            timing.close_position_ms += _elapsed_ms(close_start)
            timing.positions_closed += 1
            cash += cash_delta
            trades.append(trade)
            position = None
            equity = cash
            peak_equity = max(peak_equity, equity)
            drawdown_pct = (peak_equity - equity) / peak_equity * _D100 if peak_equity > _D0 else _D0
            force_close_point = EquityPointResult(
                trade_date=sorted_bars[-1].trade_date,
                equity=equity,
                cash=cash,
                position_value=_D0,
                drawdown_pct=drawdown_pct,
            )
            if equity_curve and equity_curve[-1].trade_date == sorted_bars[-1].trade_date:
                equity_curve[-1] = force_close_point
            else:
                equity_curve.append(force_close_point)
            self._add_warning_once(
                warnings, warning_codes, "position_force_closed",
                "An open position was force-closed because no more market data was available.",
            )
            self._add_warning_once(
                warnings, warning_codes, "data_exhausted_pricing",
                "Position force-closed at last available bar price. Actual settlement price may differ significantly.",
            )
            timing.force_closes += 1
            timing.force_close_ms += _elapsed_ms(force_close_start)

        summary_start = _time.perf_counter()
        ending_equity_f = float(equity_curve[-1].equity) if equity_curve else float(config.account_size)
        summary = build_summary(
            float(config.account_size),
            ending_equity_f,
            trades,
            equity_curve,
            risk_free_rate=config.risk_free_rate,
            risk_free_rate_curve=config.risk_free_rate_curve,
            warnings=warnings,
        )
        timing.summary_ms += _elapsed_ms(summary_start)
        timing.total_ms = _elapsed_ms(total_start)
        logger.info(
            "backtest.engine_run_timing",
            symbol=config.symbol,
            strategy_type=config.strategy_type,
            bars_input=len(bars),
            bars_processed=timing.bars_processed,
            bars_skipped_before_start=timing.bars_skipped_before_start,
            positions_opened=timing.positions_opened,
            positions_closed=timing.positions_closed,
            force_closes=timing.force_closes,
            rule_precompute_ms=round(timing.rule_precompute_ms, 3),
            mark_position_ms=round(timing.mark_position_ms, 3),
            exit_resolution_ms=round(timing.exit_resolution_ms, 3),
            build_position_ms=round(timing.build_position_ms, 3),
            build_contract_fetch_ms=round(timing.build_contract_fetch_ms, 3),
            build_contract_selector_fetch_ms=round(timing.build_contract_selector_fetch_ms, 3),
            build_contract_availability_fetch_ms=round(timing.build_contract_availability_fetch_ms, 3),
            build_contract_batch_fetch_ms=round(timing.build_contract_batch_fetch_ms, 3),
            build_contract_exact_fetch_ms=round(timing.build_contract_exact_fetch_ms, 3),
            build_contract_other_ms=round(timing.build_contract_other_ms, 3),
            build_contract_selection_cache_hits=timing.build_contract_selection_cache_hits,
            build_contract_selection_cache_misses=timing.build_contract_selection_cache_misses,
            build_contract_gateway_method_ms={
                method_name: round(elapsed_ms, 3)
                for method_name, elapsed_ms in sorted(timing.build_contract_gateway_method_ms.items())
            },
            build_contract_gateway_method_calls=dict(sorted(timing.build_contract_gateway_method_calls.items())),
            build_contract_gateway_contract_cache_hits=timing.build_contract_gateway_contract_cache_hits,
            build_contract_gateway_contract_cache_misses=timing.build_contract_gateway_contract_cache_misses,
            build_contract_gateway_exact_cache_hits=timing.build_contract_gateway_exact_cache_hits,
            build_contract_gateway_exact_cache_misses=timing.build_contract_gateway_exact_cache_misses,
            build_contract_gateway_availability_cache_hits=timing.build_contract_gateway_availability_cache_hits,
            build_contract_gateway_availability_cache_misses=timing.build_contract_gateway_availability_cache_misses,
            build_contract_gateway_availability_by_type_cache_hits=(
                timing.build_contract_gateway_availability_by_type_cache_hits
            ),
            build_contract_gateway_availability_by_type_cache_misses=(
                timing.build_contract_gateway_availability_by_type_cache_misses
            ),
            build_delta_resolution_ms=round(timing.build_delta_resolution_ms, 3),
            build_delta_iv_quote_fetch_ms=round(timing.build_delta_iv_quote_fetch_ms, 3),
            build_delta_iv_solve_ms=round(timing.build_delta_iv_solve_ms, 3),
            build_delta_kernel_ms=round(timing.build_delta_kernel_ms, 3),
            build_delta_other_ms=round(timing.build_delta_other_ms, 3),
            build_delta_lookup_cache_hits=timing.build_delta_lookup_cache_hits,
            build_delta_lookup_cache_misses=timing.build_delta_lookup_cache_misses,
            build_delta_iv_cache_hits=timing.build_delta_iv_cache_hits,
            build_delta_iv_cache_misses=timing.build_delta_iv_cache_misses,
            build_entry_quote_fetch_ms=round(timing.build_entry_quote_fetch_ms, 3),
            build_object_construction_ms=round(timing.build_object_construction_ms, 3),
            attach_quote_series_ms=round(timing.attach_quote_series_ms, 3),
            position_sizing_ms=round(timing.position_sizing_ms, 3),
            current_position_value_ms=round(timing.current_position_value_ms, 3),
            close_position_ms=round(timing.close_position_ms, 3),
            equity_curve_ms=round(timing.equity_curve_ms, 3),
            force_close_ms=round(timing.force_close_ms, 3),
            summary_ms=round(timing.summary_ms, 3),
            total_ms=round(timing.total_ms, 3),
        )
        return BacktestExecutionResult(summary=summary, trades=trades, equity_curve=equity_curve, warnings=warnings)

    def run_exit_policy_variants(
        self,
        *,
        configs: list[BacktestConfig],
        bars: list[DailyBar],
        earnings_dates: set[date],
        option_gateway: OptionDataGateway,
        ex_dividend_dates: set[date] | None = None,
        shared_entry_rule_cache: EntryRuleComputationCache | None = None,
    ) -> list[BacktestExecutionResult]:
        if not configs:
            return []
        if len(configs) == 1:
            return [
                self.run(
                    config=configs[0],
                    bars=bars,
                    earnings_dates=earnings_dates,
                    option_gateway=option_gateway,
                    ex_dividend_dates=ex_dividend_dates,
                    shared_entry_rule_cache=shared_entry_rule_cache,
                )
            ]

        timing = _EnginePhaseTiming()
        total_start = _time.perf_counter()
        base_config = configs[0]
        if base_config.strategy_type == "wheel_strategy":
            return [
                self.run(
                    config=config,
                    bars=bars,
                    earnings_dates=earnings_dates,
                    option_gateway=option_gateway,
                    ex_dividend_dates=ex_dividend_dates,
                    shared_entry_rule_cache=shared_entry_rule_cache,
                )
                for config in configs
            ]

        comparable_fields = (
            "symbol",
            "strategy_type",
            "start_date",
            "end_date",
            "target_dte",
            "dte_tolerance_days",
            "max_holding_days",
            "account_size",
            "risk_per_trade_pct",
            "commission_per_contract",
            "entry_rules",
            "risk_free_rate",
            "risk_free_rate_curve",
            "dividend_yield",
            "slippage_pct",
            "strategy_overrides",
            "custom_legs",
        )
        for config in configs[1:]:
            if any(getattr(config, name) != getattr(base_config, name) for name in comparable_fields):
                raise AppValidationError(
                    "exit-policy variants must share the same structural backtest configuration"
                )

        strategy = STRATEGY_REGISTRY.get(base_config.strategy_type)
        if strategy is None:
            raise AppValidationError(f"Unsupported strategy_type: {base_config.strategy_type}")

        sorted_bars = sorted(bars, key=lambda bar: bar.trade_date)
        pre_filter_len = len(sorted_bars)
        sorted_bars = [bar for bar in sorted_bars if bar.close_price > 0]
        if not sorted_bars:
            return [
                BacktestExecutionResult(
                    summary=build_summary(
                        float(config.account_size),
                        float(config.account_size),
                        [],
                        [],
                        risk_free_rate=config.risk_free_rate,
                        risk_free_rate_curve=config.risk_free_rate_curve,
                    ),
                    trades=[],
                    equity_curve=[],
                )
                for config in configs
            ]

        ex_dividend_dates = set(ex_dividend_dates or ())
        if not ex_dividend_dates and hasattr(option_gateway, "get_ex_dividend_dates"):
            try:
                ex_dividend_dates = option_gateway.get_ex_dividend_dates(
                    sorted_bars[0].trade_date, sorted_bars[-1].trade_date,
                )
            except Exception:
                logger.warning("engine.ex_dividend_dates_unavailable", exc_info=True)

        lanes = [
            _ExitPolicyLane(
                config=config,
                cash=Decimal(str(config.account_size)),
                peak_equity=Decimal(str(config.account_size)),
            )
            for config in configs
        ]
        for lane in lanes:
            if len(sorted_bars) < pre_filter_len:
                self._add_warning_once(
                    lane.warnings,
                    lane.warning_codes,
                    "non_positive_close_filtered",
                    f"{pre_filter_len - len(sorted_bars)} bar(s) with non-positive close price were excluded.",
                )

        evaluator = EntryRuleEvaluator(
            config=base_config,
            bars=sorted_bars,
            earnings_dates=earnings_dates,
            option_gateway=option_gateway,
            shared_cache=shared_entry_rule_cache,
        )
        entry_allowed_mask: list[bool] | None = None
        rule_precompute_start = _time.perf_counter()
        try:
            entry_allowed_mask = evaluator.build_entry_allowed_mask()
        except Exception:
            logger.warning("entry_rule_precompute_error", exc_info=True)
            for lane in lanes:
                self._add_warning_once(
                    lane.warnings,
                    lane.warning_codes,
                    "entry_rule_evaluation_error",
                    "One or more entry rule evaluations failed and were treated as not-allowed.",
                )
        timing.rule_precompute_ms += _elapsed_ms(rule_precompute_start)

        build_position_supports_custom_legs = (
            base_config.custom_legs is not None
            and "custom_legs" in inspect.signature(strategy.build_position).parameters
        )
        custom_legs = list(base_config.custom_legs) if build_position_supports_custom_legs else None
        last_bar_date = sorted_bars[-1].trade_date

        for index, bar in enumerate(sorted_bars):
            if bar.trade_date < base_config.start_date:
                timing.bars_skipped_before_start += 1
                continue
            timing.bars_processed += 1

            position_values = [_D0 for _ in lanes]
            exit_prices_by_lane: list[dict[str, float]] = [{} for _ in lanes]

            for lane_index, lane in enumerate(lanes):
                if lane.position is not None:
                    mark_start = _time.perf_counter()
                    snapshot = self._mark_position(
                        lane.position,
                        bar,
                        option_gateway,
                        lane.warnings,
                        lane.warning_codes,
                        ex_dividend_dates,
                    )
                    timing.mark_position_ms += _elapsed_ms(mark_start)
                    position_values[lane_index] = snapshot.position_value
                    exit_prices = {leg.ticker: leg.last_mid for leg in lane.position.option_legs}
                    for stock_leg in lane.position.stock_legs:
                        exit_prices[stock_leg.symbol] = stock_leg.last_price
                    exit_prices_by_lane[lane_index] = exit_prices

                    entry_cost = self._entry_value_per_unit(lane.position) * _D(lane.position.quantity)
                    if not position_values[lane_index].is_finite():
                        logger.warning("engine.nan_position_value_exit_guard", bar_date=str(bar.trade_date))
                        position_values[lane_index] = entry_cost
                    if not entry_cost.is_finite():
                        logger.warning("engine.nan_entry_cost", bar_date=str(bar.trade_date))
                        entry_cost = _D0
                        position_values[lane_index] = _D0
                    if math.isnan(lane.position.capital_required_per_unit):
                        logger.warning(
                            "engine.nan_capital_required_per_unit",
                            ticker=lane.position.display_ticker,
                            bar_date=str(bar.trade_date),
                        )
                        self._add_warning_once(
                            lane.warnings,
                            lane.warning_codes,
                            "nan_capital_required",
                            "Skipped stop/profit check: capital_required_per_unit is NaN.",
                        )
                        capital_at_risk = 0.0
                    else:
                        capital_at_risk = lane.position.capital_required_per_unit * lane.position.quantity
                    exit_start = _time.perf_counter()
                    should_exit, exit_reason = self._resolve_exit(
                        bar=bar,
                        position=lane.position,
                        max_holding_days=lane.config.max_holding_days,
                        backtest_end_date=lane.config.end_date,
                        last_bar_date=last_bar_date,
                        position_value=float(position_values[lane_index]),
                        entry_cost=float(entry_cost),
                        capital_at_risk=capital_at_risk,
                        profit_target_pct=lane.config.profit_target_pct,
                        stop_loss_pct=lane.config.stop_loss_pct,
                        current_bar_index=index,
                    )
                    timing.exit_resolution_ms += _elapsed_ms(exit_start)
                    assignment_detail = snapshot.assignment_detail
                    if snapshot.assignment_exit_reason is not None:
                        should_exit = True
                        exit_reason = snapshot.assignment_exit_reason
                    if should_exit:
                        close_start = _time.perf_counter()
                        trade, cash_delta = self._close_position(
                            lane.position,
                            lane.config,
                            position_values[lane_index],
                            bar.trade_date,
                            bar.close_price,
                            exit_prices_by_lane[lane_index],
                            exit_reason,
                            lane.warnings,
                            lane.warning_codes,
                            current_bar_index=index,
                            assignment_detail=assignment_detail,
                            trade_warnings=snapshot.warnings,
                        )
                        timing.close_position_ms += _elapsed_ms(close_start)
                        timing.positions_closed += 1
                        lane.cash += cash_delta
                        lane.trades.append(trade)
                        lane.position = None
                        position_values[lane_index] = _D0
                        exit_prices_by_lane[lane_index] = {}

            eligible_lanes: list[_ExitPolicyLane] = []
            for lane in lanes:
                just_closed_this_bar = (
                    lane.position is None
                    and len(lane.trades) > 0
                    and lane.trades[-1].exit_date == bar.trade_date
                )
                if just_closed_this_bar:
                    self._add_warning_once(
                        lane.warnings,
                        lane.warning_codes,
                        "same_day_reentry_blocked",
                        "One or more entry signals were suppressed because a position was "
                        "closed on the same trading day. The engine does not re-enter on "
                        "the same bar to avoid infinite open/close loops.",
                    )
                    continue
                if lane.position is not None or bar.trade_date > lane.config.end_date:
                    continue
                entry_allowed = False
                if entry_allowed_mask is not None:
                    entry_allowed = entry_allowed_mask[index]
                else:
                    try:
                        entry_allowed = evaluator.is_entry_allowed(index)
                    except Exception:
                        logger.warning("entry_rule_evaluation_error", bar_index=index, exc_info=True)
                        self._add_warning_once(
                            lane.warnings,
                            lane.warning_codes,
                            "entry_rule_evaluation_error",
                            "One or more entry rule evaluations failed and were treated as not-allowed.",
                        )
                if entry_allowed:
                    eligible_lanes.append(lane)

            if eligible_lanes:
                affordable_lanes = [
                    lane for lane in eligible_lanes
                    if self._can_afford_minimum_strategy_package(strategy, lane.config, bar, lane.cash)
                ]
                for lane in eligible_lanes:
                    if lane not in affordable_lanes:
                        self._add_warning_once(
                            lane.warnings,
                            lane.warning_codes,
                            "capital_requirement_exceeded",
                            "One or more signals were skipped because available cash or"
                            " configured risk budget could not support the strategy package.",
                        )

                if affordable_lanes:
                    try:
                        candidate_template = self._build_position_with_timing(
                            strategy=strategy,
                            config=base_config,
                            bar=bar,
                            bar_index=index,
                            option_gateway=option_gateway,
                            custom_legs=custom_legs,
                            timing=timing,
                        )
                    except DataUnavailableError:
                        for lane in affordable_lanes:
                            self._add_warning_once(
                                lane.warnings,
                                lane.warning_codes,
                                "missing_contract_chain",
                                "One or more entry dates could not be evaluated because no eligible"
                                " option contract chain was returned.",
                            )
                    else:
                        if candidate_template is None:
                            for lane in affordable_lanes:
                                self._add_warning_once(
                                    lane.warnings,
                                    lane.warning_codes,
                                    "missing_entry_quote",
                                    "One or more entry dates were skipped because no valid same-day option quote was returned.",
                                )
                        else:
                            ev_per_unit = self._entry_value_per_unit(candidate_template)
                            contracts_per_unit = sum(
                                leg.quantity_per_unit for leg in candidate_template.option_legs
                            )
                            gross_notional_per_unit = (
                                sum(
                                    abs(leg.entry_mid * _leg_multiplier(leg)) * leg.quantity_per_unit
                                    for leg in candidate_template.option_legs
                                )
                                + sum(
                                    abs(leg.entry_price * leg.share_quantity_per_unit)
                                    for leg in candidate_template.stock_legs
                                )
                            )
                            approved_opens: list[tuple[_ExitPolicyLane, int, Decimal]] = []
                            for lane in affordable_lanes:
                                open_start = _time.perf_counter()
                                commission_per_unit = (
                                    float(lane.config.commission_per_contract) * contracts_per_unit
                                )
                                quantity = self._resolve_position_size(
                                    available_cash=lane.cash,
                                    account_size=float(lane.config.account_size),
                                    risk_per_trade_pct=float(lane.config.risk_per_trade_pct),
                                    capital_required_per_unit=candidate_template.capital_required_per_unit,
                                    max_loss_per_unit=candidate_template.max_loss_per_unit,
                                    entry_cost_per_unit=float(abs(ev_per_unit)),
                                    commission_per_unit=commission_per_unit,
                                    slippage_pct=lane.config.slippage_pct,
                                    gross_notional_per_unit=gross_notional_per_unit,
                                )
                                if quantity <= 0:
                                    self._add_warning_once(
                                        lane.warnings,
                                        lane.warning_codes,
                                        "capital_requirement_exceeded",
                                        "One or more signals were skipped because available cash or"
                                        " configured risk budget could not support the strategy package.",
                                    )
                                    timing.position_sizing_ms += _elapsed_ms(open_start)
                                    continue
                                candidate_template.quantity = quantity
                                entry_commission = self._option_commission_total(
                                    candidate_template,
                                    lane.config.commission_per_contract,
                                )
                                slippage_cost_d = (
                                    _D(gross_notional_per_unit)
                                    * _D(quantity)
                                    * (_D(lane.config.slippage_pct) / _D100)
                                )
                                total_entry_cost = (
                                    (ev_per_unit * _D(quantity))
                                    + entry_commission
                                    + slippage_cost_d
                                )
                                if lane.cash - total_entry_cost < 0:
                                    self._add_warning_once(
                                        lane.warnings,
                                        lane.warning_codes,
                                        "negative_cash_rejected",
                                        "One or more entries were skipped because the total cost "
                                        "(including slippage) would have exceeded available cash.",
                                    )
                                    timing.position_sizing_ms += _elapsed_ms(open_start)
                                    continue
                                approved_opens.append((lane, quantity, total_entry_cost))
                                timing.position_sizing_ms += _elapsed_ms(open_start)
                            if approved_opens:
                                attach_start = _time.perf_counter()
                                self._attach_position_quote_series(
                                    candidate_template,
                                    option_gateway=option_gateway,
                                    start_date=bar.trade_date,
                                    end_date=last_bar_date,
                                )
                                timing.attach_quote_series_ms += _elapsed_ms(attach_start)
                            for lane, quantity, total_entry_cost in approved_opens:
                                candidate = self._clone_position_template(candidate_template)
                                candidate.quantity = quantity
                                candidate.detail_json.setdefault("entry_underlying_close", bar.close_price)
                                entry_commission = self._option_commission_total(
                                    candidate,
                                    lane.config.commission_per_contract,
                                )
                                candidate.entry_commission_total = entry_commission
                                lane.cash -= total_entry_cost
                                lane.position = candidate
                                timing.positions_opened += 1
                                if (
                                    strategy.margin_warning_message
                                    and candidate.capital_required_per_unit > float(abs(ev_per_unit))
                                ):
                                    self._add_warning_once(
                                        lane.warnings,
                                        lane.warning_codes,
                                        "margin_reserved",
                                        strategy.margin_warning_message,
                                    )

            all_flat_after_end = True
            for lane_index, lane in enumerate(lanes):
                if lane.position is not None:
                    current_value_start = _time.perf_counter()
                    position_values[lane_index] = self._current_position_value(lane.position, bar.close_price)
                    timing.current_position_value_ms += _elapsed_ms(current_value_start)
                    if not position_values[lane_index].is_finite():
                        logger.warning(
                            "engine.nan_position_value",
                            ticker=lane.position.display_ticker,
                            bar_date=str(bar.trade_date),
                        )
                        position_values[lane_index] = _D0
                equity_start = _time.perf_counter()
                equity = lane.cash + position_values[lane_index]
                if equity < _D0:
                    self._add_warning_once(
                        lane.warnings,
                        lane.warning_codes,
                        "negative_equity",
                        "Account equity went negative. This indicates a margin call scenario "
                        "where losses exceeded the account balance. Drawdown percentages above "
                        "100% may occur.",
                    )
                lane.peak_equity = max(lane.peak_equity, equity)
                drawdown_pct = (
                    (lane.peak_equity - equity) / lane.peak_equity * _D100
                    if lane.peak_equity > _D0
                    else _D0
                )
                lane.equity_curve.append(
                    EquityPointResult(
                        trade_date=bar.trade_date,
                        equity=equity,
                        cash=lane.cash,
                        position_value=position_values[lane_index],
                        drawdown_pct=drawdown_pct,
                    )
                )
                timing.equity_curve_ms += _elapsed_ms(equity_start)
                if lane.position is not None or bar.trade_date <= lane.config.end_date:
                    all_flat_after_end = False

            if all_flat_after_end:
                break

        results: list[BacktestExecutionResult] = []
        for lane in lanes:
            if lane.position is not None:
                force_close_start = _time.perf_counter()
                mark_start = _time.perf_counter()
                snapshot = self._mark_position(
                    lane.position,
                    sorted_bars[-1],
                    option_gateway,
                    lane.warnings,
                    lane.warning_codes,
                    ex_dividend_dates,
                )
                timing.mark_position_ms += _elapsed_ms(mark_start)
                final_position_value = snapshot.position_value
                if not final_position_value.is_finite():
                    logger.warning(
                        "engine.nan_position_value_force_close_guard",
                        bar_date=str(sorted_bars[-1].trade_date),
                    )
                    final_position_value = self._entry_value_per_unit(lane.position) * _D(lane.position.quantity)
                    if not final_position_value.is_finite():
                        final_position_value = _D0
                exit_prices_fc = {leg.ticker: leg.last_mid for leg in lane.position.option_legs}
                for stock_leg in lane.position.stock_legs:
                    exit_prices_fc[stock_leg.symbol] = stock_leg.last_price
                close_start = _time.perf_counter()
                trade, cash_delta = self._close_position(
                    lane.position,
                    lane.config,
                    final_position_value,
                    sorted_bars[-1].trade_date,
                    sorted_bars[-1].close_price,
                    exit_prices_fc,
                    "data_exhausted",
                    lane.warnings,
                    lane.warning_codes,
                    current_bar_index=len(sorted_bars) - 1,
                    trade_warnings=snapshot.warnings,
                )
                timing.close_position_ms += _elapsed_ms(close_start)
                timing.positions_closed += 1
                lane.cash += cash_delta
                lane.trades.append(trade)
                lane.position = None
                equity = lane.cash
                lane.peak_equity = max(lane.peak_equity, equity)
                drawdown_pct = (
                    (lane.peak_equity - equity) / lane.peak_equity * _D100
                    if lane.peak_equity > _D0
                    else _D0
                )
                force_close_point = EquityPointResult(
                    trade_date=sorted_bars[-1].trade_date,
                    equity=equity,
                    cash=lane.cash,
                    position_value=_D0,
                    drawdown_pct=drawdown_pct,
                )
                if lane.equity_curve and lane.equity_curve[-1].trade_date == sorted_bars[-1].trade_date:
                    lane.equity_curve[-1] = force_close_point
                else:
                    lane.equity_curve.append(force_close_point)
                self._add_warning_once(
                    lane.warnings,
                    lane.warning_codes,
                    "position_force_closed",
                    "An open position was force-closed because no more market data was available.",
                )
                self._add_warning_once(
                    lane.warnings,
                    lane.warning_codes,
                    "data_exhausted_pricing",
                    "Position force-closed at last available bar price. Actual settlement price may differ significantly.",
                )
                timing.force_closes += 1
                timing.force_close_ms += _elapsed_ms(force_close_start)

            summary_start = _time.perf_counter()
            ending_equity_f = (
                float(lane.equity_curve[-1].equity)
                if lane.equity_curve
                else float(lane.config.account_size)
            )
            summary = build_summary(
                float(lane.config.account_size),
                ending_equity_f,
                lane.trades,
                lane.equity_curve,
                risk_free_rate=lane.config.risk_free_rate,
                risk_free_rate_curve=lane.config.risk_free_rate_curve,
                warnings=lane.warnings,
            )
            results.append(
                BacktestExecutionResult(
                    summary=summary,
                    trades=lane.trades,
                    equity_curve=lane.equity_curve,
                    warnings=lane.warnings,
                )
            )
            timing.summary_ms += _elapsed_ms(summary_start)

        timing.total_ms = _elapsed_ms(total_start)
        logger.info(
            "backtest.engine_exit_variants_timing",
            symbol=base_config.symbol,
            strategy_type=base_config.strategy_type,
            lane_count=len(lanes),
            bars_input=len(bars),
            bars_processed=timing.bars_processed,
            bars_skipped_before_start=timing.bars_skipped_before_start,
            positions_opened=timing.positions_opened,
            positions_closed=timing.positions_closed,
            force_closes=timing.force_closes,
            rule_precompute_ms=round(timing.rule_precompute_ms, 3),
            mark_position_ms=round(timing.mark_position_ms, 3),
            exit_resolution_ms=round(timing.exit_resolution_ms, 3),
            build_position_ms=round(timing.build_position_ms, 3),
            build_contract_fetch_ms=round(timing.build_contract_fetch_ms, 3),
            build_contract_selector_fetch_ms=round(timing.build_contract_selector_fetch_ms, 3),
            build_contract_availability_fetch_ms=round(timing.build_contract_availability_fetch_ms, 3),
            build_contract_batch_fetch_ms=round(timing.build_contract_batch_fetch_ms, 3),
            build_contract_exact_fetch_ms=round(timing.build_contract_exact_fetch_ms, 3),
            build_contract_other_ms=round(timing.build_contract_other_ms, 3),
            build_contract_selection_cache_hits=timing.build_contract_selection_cache_hits,
            build_contract_selection_cache_misses=timing.build_contract_selection_cache_misses,
            build_contract_gateway_method_ms={
                method_name: round(elapsed_ms, 3)
                for method_name, elapsed_ms in sorted(timing.build_contract_gateway_method_ms.items())
            },
            build_contract_gateway_method_calls=dict(sorted(timing.build_contract_gateway_method_calls.items())),
            build_contract_gateway_contract_cache_hits=timing.build_contract_gateway_contract_cache_hits,
            build_contract_gateway_contract_cache_misses=timing.build_contract_gateway_contract_cache_misses,
            build_contract_gateway_exact_cache_hits=timing.build_contract_gateway_exact_cache_hits,
            build_contract_gateway_exact_cache_misses=timing.build_contract_gateway_exact_cache_misses,
            build_contract_gateway_availability_cache_hits=timing.build_contract_gateway_availability_cache_hits,
            build_contract_gateway_availability_cache_misses=timing.build_contract_gateway_availability_cache_misses,
            build_contract_gateway_availability_by_type_cache_hits=(
                timing.build_contract_gateway_availability_by_type_cache_hits
            ),
            build_contract_gateway_availability_by_type_cache_misses=(
                timing.build_contract_gateway_availability_by_type_cache_misses
            ),
            build_delta_resolution_ms=round(timing.build_delta_resolution_ms, 3),
            build_delta_iv_quote_fetch_ms=round(timing.build_delta_iv_quote_fetch_ms, 3),
            build_delta_iv_solve_ms=round(timing.build_delta_iv_solve_ms, 3),
            build_delta_kernel_ms=round(timing.build_delta_kernel_ms, 3),
            build_delta_other_ms=round(timing.build_delta_other_ms, 3),
            build_delta_lookup_cache_hits=timing.build_delta_lookup_cache_hits,
            build_delta_lookup_cache_misses=timing.build_delta_lookup_cache_misses,
            build_delta_iv_cache_hits=timing.build_delta_iv_cache_hits,
            build_delta_iv_cache_misses=timing.build_delta_iv_cache_misses,
            build_entry_quote_fetch_ms=round(timing.build_entry_quote_fetch_ms, 3),
            build_object_construction_ms=round(timing.build_object_construction_ms, 3),
            attach_quote_series_ms=round(timing.attach_quote_series_ms, 3),
            position_sizing_ms=round(timing.position_sizing_ms, 3),
            current_position_value_ms=round(timing.current_position_value_ms, 3),
            close_position_ms=round(timing.close_position_ms, 3),
            equity_curve_ms=round(timing.equity_curve_ms, 3),
            force_close_ms=round(timing.force_close_ms, 3),
            summary_ms=round(timing.summary_ms, 3),
            total_ms=round(timing.total_ms, 3),
        )
        return results

    def _mark_position(
        self,
        position: OpenMultiLegPosition,
        bar: DailyBar,
        option_gateway: OptionDataGateway,
        warnings: list[dict[str, Any]],
        warning_codes: set[str],
        ex_dividend_dates: set[date] | None = None,
    ) -> PositionSnapshot:
        """Re-price all legs at the current bar and return a value snapshot.

        IMPORTANT: This method intentionally mutates ``leg.last_mid`` and
        ``leg.last_price`` in-place.  The engine loop relies on these updated
        values for carry-forward pricing when a quote is missing on a
        subsequent day.  Do not replace with a copy-on-write pattern unless
        you also update the carry-forward logic in ``get_quote is None``
        branches above and in ``_close_position``.
        """
        option_value = _D0
        position_quantity = _D(position.quantity)
        missing_quote_tickers: list[str] = []
        quote_lookup = self._load_option_quotes_for_bar(
            position=position,
            bar=bar,
            option_gateway=option_gateway,
        )
        for leg in position.option_legs:
            current_mid = self._resolve_option_mid_from_quote(
                leg,
                bar,
                quote_lookup.get(leg.ticker),
                missing_quote_tickers,
            )
            leg.last_mid = current_mid
            option_value += _option_signed_unit_factor(leg) * _D(current_mid) * position_quantity

        if missing_quote_tickers:
            self._add_warning_once(
                warnings,
                warning_codes,
                "missing_option_mark_quote",
                "One or more daily option marks were missing; the engine carried forward the previous mid-price.",
            )

        stock_value = _D0
        close_price_d = _D(bar.close_price)
        for leg in position.stock_legs:
            leg.last_price = bar.close_price
            stock_value += _stock_signed_unit_factor(leg) * close_price_d * position_quantity

        assignment_exit_reason, assignment_detail = self._check_early_assignment(
            position=position,
            bar=bar,
            ex_dividend_dates=ex_dividend_dates or set(),
        )
        if assignment_detail is not None:
            assigned_leg = assignment_detail["assigned_leg"]
            exit_mid = assignment_detail["settlement_price"]
            for leg in position.option_legs:
                if leg.ticker == assigned_leg:
                    leg.last_mid = exit_mid
            option_value = self._assignment_position_value(position, assignment_detail)

        return PositionSnapshot(
            position_value=option_value + stock_value,
            position_missing_quote=bool(missing_quote_tickers),
            missing_quote_tickers=tuple(missing_quote_tickers),
            assignment_exit_reason=assignment_exit_reason,
            assignment_detail=assignment_detail,
            warnings=((assignment_detail["warning_message"],) if assignment_detail is not None else ()),
        )

    @classmethod
    def _assignment_position_value(
        cls,
        position: OpenMultiLegPosition,
        assignment_detail: dict[str, Any],
    ) -> Decimal:
        assigned_ticker = assignment_detail["assigned_leg"]
        settlement_price = _D(assignment_detail["settlement_price"])
        option_value = _D0
        position_quantity = _D(position.quantity)
        for leg in position.option_legs:
            leg_mid = settlement_price if leg.ticker == assigned_ticker else _D(leg.last_mid)
            option_value += _option_signed_unit_factor(leg) * leg_mid * position_quantity
        return option_value

    @classmethod
    def _check_early_assignment(
        cls,
        *,
        position: OpenMultiLegPosition,
        bar: DailyBar,
        ex_dividend_dates: set[date],
    ) -> tuple[str | None, dict[str, Any] | None]:
        next_day = bar.trade_date + timedelta(days=1)
        for leg in position.option_legs:
            if leg.side >= 0:
                continue
            intrinsic = float(cls._intrinsic_value(leg.contract_type, leg.strike_price, bar.close_price))
            if intrinsic <= 0:
                continue
            time_value = max(0.0, leg.last_mid - intrinsic)
            dte = max(0, (leg.expiration_date - bar.trade_date).days)
            moneyness = (bar.close_price / leg.strike_price) if leg.contract_type == "call" else (leg.strike_price / bar.close_price)

            if leg.contract_type == "call" and next_day in ex_dividend_dates and intrinsic >= 2.0 and time_value <= 0.25:
                return "early_assignment_call_ex_div", {
                    "assignment": True,
                    "assignment_trigger": "ex_dividend",
                    "assigned_leg": leg.ticker,
                    "contract_type": leg.contract_type,
                    "intrinsic_value": intrinsic,
                    "time_value": time_value,
                    "days_to_expiration": dte,
                    "next_ex_dividend_date": next_day.isoformat(),
                    "settlement_price": intrinsic,
                    "warning_message": (
                        f"Short call {leg.ticker} was treated as early-assigned on "
                        f"{bar.trade_date.isoformat()} before the {next_day.isoformat()} ex-dividend date."
                    ),
                }

            if leg.contract_type == "put" and dte <= 3 and moneyness >= 1.05 and time_value <= 0.1:
                return "early_assignment_put_deep_itm", {
                    "assignment": True,
                    "assignment_trigger": "deep_itm_put",
                    "assigned_leg": leg.ticker,
                    "contract_type": leg.contract_type,
                    "intrinsic_value": intrinsic,
                    "time_value": time_value,
                    "days_to_expiration": dte,
                    "moneyness_ratio": moneyness,
                    "settlement_price": intrinsic,
                    "warning_message": (
                        f"Short put {leg.ticker} was treated as early-assigned on "
                        f"{bar.trade_date.isoformat()} due to deep ITM intrinsic value near expiry."
                    ),
                }

        return None, None

    @staticmethod
    def _load_option_quotes_for_bar(
        *,
        position: OpenMultiLegPosition,
        bar: DailyBar,
        option_gateway: OptionDataGateway,
    ) -> dict[str, OptionQuoteRecord | None]:
        tickers = [leg.ticker for leg in position.option_legs]
        quote_series_lookup = position.quote_series_lookup
        loaded_tickers = position.quote_series_loaded_tickers
        if quote_series_lookup and loaded_tickers:
            quotes: dict[str, OptionQuoteRecord | None] = {}
            missing: list[str] = []
            for ticker in tickers:
                if ticker in loaded_tickers:
                    quotes[ticker] = quote_series_lookup.get(ticker, {}).get(bar.trade_date)
                else:
                    missing.append(ticker)
            if not missing:
                return quotes
        else:
            quotes = {}
            missing = tickers
        batch_fetch = getattr(option_gateway, "get_quotes", None)
        if callable(batch_fetch) and missing:
            try:
                fetched = dict(batch_fetch(missing, bar.trade_date))
                for ticker, quote in fetched.items():
                    quotes[ticker] = quote
                    if quote_series_lookup is not None:
                        quote_series_lookup.setdefault(ticker, {})[bar.trade_date] = quote
                return quotes
            except Exception:
                logger.warning("engine.batch_quote_fetch_failed", trade_date=str(bar.trade_date), exc_info=True)
        for ticker in missing:
            quote = option_gateway.get_quote(ticker, bar.trade_date)
            quotes[ticker] = quote
            if quote_series_lookup is not None:
                quote_series_lookup.setdefault(ticker, {})[bar.trade_date] = quote
        return quotes

    @staticmethod
    def _attach_position_quote_series(
        position: OpenMultiLegPosition,
        *,
        option_gateway: OptionDataGateway,
        start_date: date,
        end_date: date,
    ) -> None:
        if position.quote_series_lookup or not position.option_legs or end_date < start_date:
            return
        fetch_series = getattr(option_gateway, "get_quote_series", None)
        if not callable(fetch_series):
            return
        tickers = [leg.ticker for leg in position.option_legs]
        try:
            tickers = [leg.ticker for leg in position.option_legs]
            position.quote_series_lookup = {
                ticker: dict(quotes_by_date)
                for ticker, quotes_by_date in fetch_series(tickers, start_date, end_date).items()
            }
            position.quote_series_loaded_tickers = set(tickers)
        except Exception:
            logger.warning(
                "engine.batch_quote_series_fetch_failed",
                trade_date=str(start_date),
                end_date=str(end_date),
                exc_info=True,
            )

    def _resolve_option_mid(
        self,
        leg: Any,
        bar: DailyBar,
        option_gateway: OptionDataGateway,
        missing_quote_tickers: list[str],
    ) -> float:
        """Determine the current mid-price for a single option leg.

        Returns the best available price: live quote > intrinsic value (if
        expired) > carry-forward from last known mid.  Appends the ticker to
        *missing_quote_tickers* when a live quote was unavailable.
        """
        quote = option_gateway.get_quote(leg.ticker, bar.trade_date)
        return self._resolve_option_mid_from_quote(leg, bar, quote, missing_quote_tickers)

    def _resolve_option_mid_from_quote(
        self,
        leg: Any,
        bar: DailyBar,
        quote: OptionQuoteRecord | None,
        missing_quote_tickers: list[str],
    ) -> float:
        if quote is None:
            if bar.trade_date >= leg.expiration_date:
                return float(self._intrinsic_value(
                    leg.contract_type, leg.strike_price, bar.close_price,
                ))
            missing_quote_tickers.append(leg.ticker)
            return leg.last_mid
        if quote.mid_price is None or not math.isfinite(quote.mid_price):
            if bar.trade_date >= leg.expiration_date:
                return float(self._intrinsic_value(
                    leg.contract_type, leg.strike_price, bar.close_price,
                ))
            missing_quote_tickers.append(leg.ticker)
            return leg.last_mid
        if quote.mid_price <= 0:
            if bar.trade_date >= leg.expiration_date:
                return float(self._intrinsic_value(
                    leg.contract_type, leg.strike_price, bar.close_price,
                ))
            missing_quote_tickers.append(leg.ticker)
            return leg.last_mid
        return quote.mid_price

    @staticmethod
    def _current_position_value(position: OpenMultiLegPosition, underlying_close: float) -> Decimal:
        position_quantity = _D(position.quantity)
        option_value = _D0
        for leg in position.option_legs:
            option_value += _option_signed_unit_factor(leg) * _D(leg.last_mid) * position_quantity
        stock_value = _D0
        underlying_close_d = _D(underlying_close)
        for leg in position.stock_legs:
            stock_value += _stock_signed_unit_factor(leg) * underlying_close_d * position_quantity
        result = option_value + stock_value
        if not result.is_finite():
            return _D0
        return result

    @staticmethod
    def _resolve_position_size(
        available_cash: Decimal | float,
        account_size: float,
        risk_per_trade_pct: float,
        capital_required_per_unit: float,
        max_loss_per_unit: float | None,
        entry_cost_per_unit: float = 0.0,
        commission_per_unit: float = 0.0,
        slippage_pct: float = 0.0,
        gross_notional_per_unit: float = 0.0,
    ) -> int:
        d_cash = _D(available_cash) if not isinstance(available_cash, Decimal) else available_cash
        d_account = _D(account_size)
        d_risk_pct = _D(risk_per_trade_pct)
        d_cap_req = _D(capital_required_per_unit)
        if d_cap_req < _D0:
            return 0
        _MIN_CAPITAL_PER_UNIT = _D("50")
        if d_cap_req < _MIN_CAPITAL_PER_UNIT:
            d_cap_req = _MIN_CAPITAL_PER_UNIT
        risk_budget = d_account * (d_risk_pct / _D100)
        d_max_loss = _D(max_loss_per_unit) if max_loss_per_unit is not None and max_loss_per_unit > 0 else d_cap_req
        effective_risk = max(d_max_loss, _D("1"))
        by_risk = int(risk_budget // effective_risk)
        d_gross_notional = _D(gross_notional_per_unit)
        d_entry_cost = _D(entry_cost_per_unit)
        d_slip_pct = _D(slippage_pct) / _D100
        slippage_base = d_gross_notional if d_gross_notional > _D0 else d_entry_cost
        slippage_per_unit = slippage_base * d_slip_pct
        d_commission = _D(commission_per_unit)
        cash_per_unit = max(d_cap_req, d_entry_cost) + d_commission + slippage_per_unit
        if cash_per_unit <= _D0:
            return 0
        by_cash = int(d_cash // cash_per_unit)
        return max(0, min(by_risk, by_cash))

    def _close_position(
        self,
        position: OpenMultiLegPosition,
        config: BacktestConfig,
        exit_value: Decimal | float | int,
        exit_date: date,
        exit_underlying_close: float,
        exit_prices: dict[str, float],
        exit_reason: str,
        warnings: list[dict[str, Any]] | None = None,
        warning_codes: set[str] | None = None,
        current_bar_index: int | None = None,
        assignment_detail: dict[str, Any] | None = None,
        trade_warnings: tuple[str, ...] = (),
    ) -> tuple[TradeResult, Decimal]:
        """Close a position and return the TradeResult and cash change (exit proceeds minus exit commission)."""
        exit_value_d = exit_value if isinstance(exit_value, Decimal) else _D(exit_value)
        (
            exit_commission,
            option_exit_notional,
            commission_waivers,
        ) = self._option_exit_cost_profile(
            position=position,
            commission_per_contract=config.commission_per_contract,
            exit_date=exit_date,
            exit_prices=exit_prices,
            assignment_detail=assignment_detail,
        )
        position_quantity = _D(position.quantity)
        stock_exit_notional: Decimal = (
            sum(abs(_D(leg.last_price) * _stock_abs_unit_factor(leg)) for leg in position.stock_legs) * position_quantity
            if position.stock_legs else _D0
        )
        exit_gross_notional = option_exit_notional + stock_exit_notional
        slippage_pct_d = _D(config.slippage_pct) / _D100
        exit_slippage = exit_gross_notional * slippage_pct_d
        entry_value_per_unit = self._entry_value_per_unit(position)
        option_entry_notional: Decimal = sum(
            abs(_D(leg.entry_mid) * _option_abs_unit_factor(leg))
            for leg in position.option_legs
        ) or _D0
        option_entry_notional *= position_quantity
        stock_entry_notional: Decimal = (
            sum(abs(_D(leg.entry_price) * _stock_abs_unit_factor(leg)) for leg in position.stock_legs) * position_quantity
            if position.stock_legs else _D0
        )
        entry_gross_notional = option_entry_notional + stock_entry_notional
        entry_slippage = entry_gross_notional * slippage_pct_d
        cash_delta = exit_value_d - exit_commission - exit_slippage
        exit_value_per_unit = exit_value_d / position_quantity if position.quantity else _D0
        gross_pnl = (exit_value_per_unit - entry_value_per_unit) * position_quantity
        dividends_received = self._estimate_dividends_received(position, config, exit_date)
        if dividends_received:
            gross_pnl += dividends_received
            cash_delta += dividends_received
        entry_commission_total = (
            position.entry_commission_total
            if isinstance(position.entry_commission_total, Decimal)
            else _D(position.entry_commission_total)
        )
        total_commissions = entry_commission_total + exit_commission
        total_slippage = entry_slippage + exit_slippage
        net_pnl = gross_pnl - total_commissions - total_slippage

        nan_fields: list[str] = []
        for name, val in [("gross_pnl", gross_pnl), ("net_pnl", net_pnl), ("exit_value_per_unit", exit_value_per_unit)]:
            if not val.is_finite():
                logger.warning("engine.non_finite_trade_value", field=name, value=str(val), ticker=position.display_ticker)
                nan_fields.append(name)
        if not gross_pnl.is_finite():
            gross_pnl = _D0
        if not net_pnl.is_finite():
            net_pnl = _D0
        if not exit_value_per_unit.is_finite():
            exit_value_per_unit = _D0
        if not total_commissions.is_finite():
            nan_fields.append("total_commissions")
            total_commissions = _D0
        if not total_slippage.is_finite():
            nan_fields.append("total_slippage")
            total_slippage = _D0
        if not cash_delta.is_finite():
            nan_fields.append("cash_delta")
            cash_delta = _D0
        if nan_fields and warnings is not None and warning_codes is not None:
            self._add_warning_once(
                warnings, warning_codes, "nan_in_trade_values",
                f"One or more trade values ({', '.join(nan_fields)}) were NaN/Inf and "
                f"were zeroed. This may indicate poor data quality for {position.display_ticker}.",
            )
        expiration_date = position.scheduled_exit_date or (
            max(leg.expiration_date for leg in position.option_legs)
            if position.option_legs
            else exit_date
        )
        trading_days_held = (
            (current_bar_index - position.entry_index) if current_bar_index is not None else None
        )
        trade = TradeResult(
            option_ticker=position.display_ticker,
            strategy_type=config.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=position.entry_date,
            exit_date=exit_date,
            expiration_date=expiration_date,
            quantity=position.quantity,
            dte_at_open=position.dte_at_open,
            holding_period_days=(exit_date - position.entry_date).days,
            holding_period_trading_days=trading_days_held,
            entry_underlying_close=_D(self._entry_underlying_close(position)),
            exit_underlying_close=_D(exit_underlying_close),
            entry_mid=entry_value_per_unit / _D100,
            exit_mid=exit_value_per_unit / _D100,
            gross_pnl=gross_pnl,
            net_pnl=net_pnl,
            total_commissions=total_commissions,
            entry_reason=position.entry_reason,
            exit_reason=exit_reason,
            detail_json={
                **self._build_trade_detail_json(position, exit_prices, float(exit_value_per_unit), assignment_detail),
                "unit_convention": "per_unit_divided_by_100",
                "entry_commissions": float(entry_commission_total),
                "exit_commissions": float(exit_commission),
                "commission_waivers": commission_waivers,
                "total_slippage": float(total_slippage),
                "entry_slippage": float(entry_slippage),
                "exit_slippage": float(exit_slippage),
                "dividends_received": float(dividends_received),
            },
            warnings=trade_warnings,
        )
        if assignment_detail is not None and warnings is not None and warning_codes is not None:
            self._add_warning_once(
                warnings,
                warning_codes,
                assignment_detail["assignment_trigger"],
                assignment_detail["warning_message"],
            )
        return trade, cash_delta

    def _estimate_dividends_received(
        self,
        position: OpenMultiLegPosition,
        config: BacktestConfig,
        exit_date: date,
    ) -> Decimal:
        """Approximate dividends for stock legs using annualized dividend_yield.

        This is a pragmatic correction for stock-holding strategies until
        per-ex-dividend cash amounts are available from market-data sources.
        """
        if not position.stock_legs or config.dividend_yield <= 0:
            return _D0
        holding_days = max((exit_date - position.entry_date).days, 0)
        if holding_days == 0:
            return _D0
        annual_yield = _D(config.dividend_yield)
        proration = _D(holding_days) / _D365
        dividends = _D0
        position_quantity = _D(position.quantity)
        for leg in position.stock_legs:
            notional = _D(leg.entry_price) * _stock_abs_unit_factor(leg) * position_quantity
            dividends += notional * annual_yield * proration * _D(leg.side)
        return dividends

    @staticmethod
    def _entry_value_per_unit(position: OpenMultiLegPosition) -> Decimal:
        option_value = _D0
        for leg in position.option_legs:
            option_value += _option_signed_unit_factor(leg) * _D(leg.entry_mid)
        stock_value = _D0
        for leg in position.stock_legs:
            stock_value += _stock_signed_unit_factor(leg) * _D(leg.entry_price)
        return option_value + stock_value

    @staticmethod
    def _entry_underlying_close(position: OpenMultiLegPosition) -> float:
        if position.stock_legs:
            return position.stock_legs[0].entry_price
        value = position.detail_json.get("entry_underlying_close")
        if value is not None and value != 0.0:
            return float(value)
        if position.option_legs:
            strikes = [leg.strike_price for leg in position.option_legs if leg.strike_price > 0]
            if strikes:
                fallback = sum(strikes) / len(strikes)
                logger.warning(
                    "engine.missing_entry_underlying_close",
                    ticker=position.display_ticker,
                    fallback=fallback,
                )
                return fallback
        return 0.0

    @staticmethod
    def _option_commission_total(
        position: OpenMultiLegPosition,
        commission_per_contract: Decimal | float | int,
    ) -> Decimal:
        contracts_per_unit = sum(leg.quantity_per_unit for leg in position.option_legs)
        commission = (
            commission_per_contract
            if isinstance(commission_per_contract, Decimal)
            else _D(commission_per_contract)
        )
        return commission * _D(contracts_per_unit) * _D(position.quantity)

    @classmethod
    def _option_exit_cost_profile(
        cls,
        *,
        position: OpenMultiLegPosition,
        commission_per_contract: Decimal | float | int,
        exit_date: date,
        exit_prices: dict[str, float],
        assignment_detail: dict[str, Any] | None = None,
    ) -> tuple[Decimal, Decimal, list[dict[str, Any]]]:
        commission = (
            commission_per_contract
            if isinstance(commission_per_contract, Decimal)
            else _D(commission_per_contract)
        )
        assigned_ticker = assignment_detail["assigned_leg"] if assignment_detail is not None else None
        total_commission = _D0
        slippage_notional = _D0
        waivers: list[dict[str, Any]] = []
        position_quantity = _D(position.quantity)

        for leg in position.option_legs:
            leg_exit_mid = _D(exit_prices.get(leg.ticker, leg.last_mid))
            leg_contracts = _D(leg.quantity_per_unit) * position_quantity
            leg_notional = abs(leg_exit_mid * _option_abs_unit_factor(leg)) * position_quantity
            waiver_reason: str | None = None

            if assigned_ticker == leg.ticker:
                waiver_reason = "assignment_or_exercise"
            elif leg.expiration_date <= exit_date:
                waiver_reason = "expired_or_settled"
            elif leg.side < 0 and leg_exit_mid <= _D_FIVE_CENTS:
                waiver_reason = "buy_to_close_0.05_or_less"

            if waiver_reason is None:
                total_commission += commission * leg_contracts
                slippage_notional += leg_notional
                continue

            waivers.append(
                {
                    "ticker": leg.ticker,
                    "reason": waiver_reason,
                    "contracts": int(leg.quantity_per_unit * position.quantity),
                    "exit_mid": float(leg_exit_mid),
                }
            )
            if waiver_reason == "buy_to_close_0.05_or_less":
                slippage_notional += leg_notional

        return total_commission, slippage_notional, waivers

    @staticmethod
    def _build_trade_detail_json(
        position: OpenMultiLegPosition,
        exit_prices: dict[str, float],
        exit_value_per_unit: float,
        assignment_detail: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        detail = dict(position.detail_json)
        legs = [dict(leg) for leg in detail.get("legs", [])]
        for leg in legs:
            ticker = leg.get("ticker")
            if isinstance(ticker, str) and ticker in exit_prices:
                leg["exit_mid"] = exit_prices[ticker]
        for leg in legs:
            if leg.get("asset_type") == "stock":
                identifier = leg.get("identifier")
                if isinstance(identifier, str) and identifier in exit_prices:
                    leg["exit_price"] = exit_prices[identifier]
        detail["legs"] = legs
        detail["actual_units"] = position.quantity

        cap_req = position.capital_required_per_unit * position.quantity
        detail["capital_required_total"] = None if (isinstance(cap_req, float) and not math.isfinite(cap_req)) else cap_req

        if position.max_loss_per_unit is None:
            detail["max_loss_total"] = None
        else:
            ml = position.max_loss_per_unit * position.quantity
            detail["max_loss_total"] = None if (isinstance(ml, float) and not math.isfinite(ml)) else ml

        if position.max_profit_per_unit is None:
            detail["max_profit_total"] = None
        else:
            mp = position.max_profit_per_unit * position.quantity
            detail["max_profit_total"] = None if (isinstance(mp, float) and not math.isfinite(mp)) else mp
        detail["exit_package_market_value"] = exit_value_per_unit
        if assignment_detail is not None:
            detail["assignment"] = True
            detail["assignment_detail"] = {
                key: value for key, value in assignment_detail.items() if key != "warning_message"
            }
        return detail

    @staticmethod
    def _resolve_exit(
        bar: DailyBar,
        position: OpenMultiLegPosition,
        max_holding_days: int,
        backtest_end_date: date,
        last_bar_date: date,
        *,
        position_value: float = 0.0,
        entry_cost: float = 0.0,
        capital_at_risk: float = 0.0,
        profit_target_pct: float | None = None,
        stop_loss_pct: float | None = None,
        current_bar_index: int | None = None,
    ) -> tuple[bool, str]:
        if max_holding_days < 1:
            logger.warning("engine.max_holding_days_clamped", original=max_holding_days)
            max_holding_days = 1
        # NOTE: The wheel strategy has its own exit logic (assignment-based
        # rolling) that does not go through this method. See
        # backtests/strategies/wheel.py for that path.
        if position.option_legs:
            exit_date = position.scheduled_exit_date or max(leg.expiration_date for leg in position.option_legs)
        else:
            exit_date = position.scheduled_exit_date or (position.entry_date + timedelta(days=max_holding_days))
        if bar.trade_date >= exit_date:
            return True, "expiration"

        if capital_at_risk > 0 and (stop_loss_pct is not None or profit_target_pct is not None):
            unrealized_pnl = position_value - entry_cost
            unrealized_pnl_pct = (unrealized_pnl / capital_at_risk) * 100.0

            if stop_loss_pct is not None and unrealized_pnl_pct <= -stop_loss_pct:
                return True, "stop_loss"
            if profit_target_pct is not None and unrealized_pnl_pct >= profit_target_pct:
                return True, "profit_target"

        # Count trading days (bars) instead of calendar days to avoid
        # premature exits over weekends and holidays.
        if current_bar_index is not None:
            trading_days_held = current_bar_index - position.entry_index
            if trading_days_held >= max_holding_days:
                return True, "max_holding_days"
        else:
            if (bar.trade_date - position.entry_date).days >= max_holding_days:
                return True, "max_holding_days"

        if bar.trade_date >= backtest_end_date and bar.trade_date == last_bar_date:
            return True, "backtest_end"
        return False, ""

    @staticmethod
    def _intrinsic_value(contract_type: str, strike_price: float, underlying_close: float) -> Decimal:
        if contract_type == "call":
            return max(_D0, _D(underlying_close) - _D(strike_price))
        return max(_D0, _D(strike_price) - _D(underlying_close))

    @staticmethod
    def _estimate_realized_vol(bars: list, lookback: int = 60) -> float | None:
        """Estimate annualized realized volatility from recent close prices."""
        closes = [b.close_price for b in bars[-lookback:] if b.close_price > 0]
        if len(closes) < 20:
            return None
        log_returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0]
        if len(log_returns) < 10:
            return None
        mean_r = sum(log_returns) / len(log_returns)
        variance = sum((r - mean_r) ** 2 for r in log_returns) / (len(log_returns) - 1)
        return math.sqrt(variance * 252)

    @staticmethod
    def _add_warning_once(
        warnings: list[dict[str, Any]],
        warning_codes: set[str],
        code: str,
        message: str,
    ) -> None:
        if code in warning_codes:
            return
        warning_codes.add(code)
        warnings.append({"code": code, "message": message})
