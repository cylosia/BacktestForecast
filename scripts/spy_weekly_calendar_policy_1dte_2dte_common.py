from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.rules import EntryRuleComputationCache  # noqa: E402
from backtestforecast.backtests.types import (  # noqa: E402
    BacktestConfig,
    RiskFreeRateCurve,
    TradeResult,
    estimate_risk_free_rate,
)
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.market_data.service import HistoricalDataBundle  # noqa: E402
from backtestforecast.schemas.backtests import (  # noqa: E402
    StrategyOverrides,
    StrategyType,
    StrikeSelection,
    StrikeSelectionMode,
)


DEFAULT_SYMBOL = "SPY"
DEFAULT_TRAIN_START_DATE = date(2024, 1, 1)
REQUESTED_END_DATE = date(2025, 12, 31)
STARTING_EQUITY = 100_000.0
TARGET_DTE = 1
FAR_LEG_TARGET_DTE = 2
# Preserve the existing Friday-entry workflow. A 3-day tolerance allows Friday
# entries to resolve into SPY's next available daily expirations around weekends.
DTE_TOLERANCE_DAYS = 3
MAX_HOLDING_DAYS = 10

DEFAULT_BATCH_RUN_LABEL = "spy_1dte_2dte_daily_train2y_20240101_20251231"
DEFAULT_BATCH_SUMMARY_CSV = (
    ROOT
    / "logs"
    / "batch"
    / "weekly_calendar_policy_two_stage"
    / DEFAULT_BATCH_RUN_LABEL
    / "summary.csv"
)
CACHE_ROOT = ROOT / "logs" / "search_cache" / "weekly_calendar_policy_two_stage_spy_1dte_2dte"


def build_daily_entry_dates(*, bars: list[object], start_date: date, end_date: date) -> list[date]:
    return [
        bar.trade_date
        for bar in bars
        if start_date <= bar.trade_date <= end_date
    ]


def build_daily_entry_dates_from_indicator_index(*, indicator_dates: list[date], start_date: date, end_date: date) -> list[date]:
    return [trade_date for trade_date in indicator_dates if start_date <= trade_date <= end_date]


@dataclass(frozen=True, slots=True)
class FilterConfig:
    roc_threshold: float
    adx_threshold: float
    rsi_threshold: float | None

    @property
    def label(self) -> str:
        rsi_part = "none" if self.rsi_threshold is None else str(int(self.rsi_threshold))
        return f"roc{int(self.roc_threshold)}_adx{int(self.adx_threshold)}_rsi{rsi_part}"

    def matches(self, indicators: dict[str, float | None] | None) -> bool:
        if indicators is None:
            return False
        roc63 = indicators.get("roc63")
        adx14 = indicators.get("adx14")
        rsi14 = indicators.get("rsi14")
        if not isinstance(roc63, float) or roc63 <= self.roc_threshold:
            return False
        adx_ok = isinstance(adx14, float) and adx14 > self.adx_threshold
        if self.rsi_threshold is None:
            return adx_ok
        rsi_ok = isinstance(rsi14, float) and rsi14 > self.rsi_threshold
        return adx_ok or rsi_ok


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    label: str
    symbol: str
    strategy_type: StrategyType
    delta_target: int
    profit_target_pct: int


def _load_risk_free_curve(
    store: HistoricalMarketDataStore,
    *,
    start_date: date,
    end_date: date,
) -> RiskFreeRateCurve:
    local_series = store.get_treasury_yield_series(start_date, end_date)
    default_rate = store.get_average_treasury_yield(start_date, start_date)
    if default_rate is None:
        default_rate = estimate_risk_free_rate(start_date, end_date)
    if local_series:
        ordered_dates = tuple(sorted(local_series))
        ordered_rates = tuple(float(local_series[trade_date]) for trade_date in ordered_dates)
        return RiskFreeRateCurve(
            default_rate=float(default_rate),
            dates=ordered_dates,
            rates=ordered_rates,
        )
    return RiskFreeRateCurve(default_rate=float(default_rate))


def _build_bundle(
    store: HistoricalMarketDataStore,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
) -> HistoricalDataBundle:
    warmup_start = start_date - timedelta(days=210 * 3)
    return HistoricalDataBundle(
        bars=store.get_underlying_day_bars(symbol, warmup_start, end_date),
        earnings_dates=store.list_earnings_event_dates(symbol, warmup_start, end_date),
        ex_dividend_dates=store.list_ex_dividend_dates(symbol, warmup_start, end_date),
        option_gateway=HistoricalOptionGateway(store, symbol),
        data_source="local",
        entry_rule_cache=EntryRuleComputationCache(),
    )


def _build_calendar_config(
    *,
    strategy: StrategyConfig,
    entry_date: date,
    latest_available_date: date,
    risk_free_curve: RiskFreeRateCurve,
) -> BacktestConfig:
    if strategy.strategy_type == StrategyType.CALENDAR_SPREAD:
        overrides = StrategyOverrides(
            calendar_far_leg_target_dte=FAR_LEG_TARGET_DTE,
            short_call_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(strategy.delta_target)),
            ),
        )
    else:
        overrides = StrategyOverrides(
            calendar_far_leg_target_dte=FAR_LEG_TARGET_DTE,
            short_put_strike=StrikeSelection(
                mode=StrikeSelectionMode.DELTA_TARGET,
                value=Decimal(str(strategy.delta_target)),
            ),
        )
    return BacktestConfig(
        symbol=strategy.symbol,
        strategy_type=strategy.strategy_type.value,
        start_date=entry_date,
        end_date=min(latest_available_date, entry_date + timedelta(days=35)),
        target_dte=TARGET_DTE,
        dte_tolerance_days=DTE_TOLERANCE_DAYS,
        max_holding_days=MAX_HOLDING_DAYS,
        account_size=Decimal(str(STARTING_EQUITY)),
        risk_per_trade_pct=Decimal("100"),
        commission_per_contract=Decimal("0"),
        entry_rules=[],
        risk_free_rate=risk_free_curve.default_rate,
        risk_free_rate_curve=risk_free_curve,
        dividend_yield=0.0,
        slippage_pct=0.0,
        strategy_overrides=overrides,
        profit_target_pct=float(strategy.profit_target_pct),
        stop_loss_pct=None,
    )


def _trade_roi_on_margin_pct(trade: TradeResult) -> float | None:
    capital_required = trade.detail_json.get("capital_required_per_unit")
    if capital_required is None:
        return None
    total_capital = float(capital_required) * float(trade.quantity or 1)
    if total_capital <= 0:
        return None
    return float(trade.net_pnl) / total_capital * 100.0
