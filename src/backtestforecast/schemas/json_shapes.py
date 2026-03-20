"""Lightweight shape validators for JSONB blobs that flow to the frontend.

These use TypedDict for documentation + a log-only validator so that malformed
data is surfaced early without crashing persisted rows.
"""

from __future__ import annotations

from typing import Any, TypedDict

import structlog

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# BacktestTrade.detail_json
# ---------------------------------------------------------------------------

class TradeDetailShape(TypedDict, total=False):
    entry_underlying_close: float | None
    exit_underlying_close: float | None
    net_cost_per_unit: float | None
    entry_mid: float | None
    exit_mid: float | None
    margin_per_unit: float | None
    option_legs: list[dict[str, Any]]
    stock_legs: list[dict[str, Any]]


_TRADE_DETAIL_REQUIRED_KEYS: frozenset[str] = frozenset({"entry_mid", "exit_mid"})


# ---------------------------------------------------------------------------
# Regime snapshot (SymbolAnalysis.regime_json)
# ---------------------------------------------------------------------------

class RegimeShape(TypedDict, total=False):
    regimes: list[str]
    rsi_14: float | None
    ema_8: float | None
    ema_21: float | None
    sma_50: float | None
    sma_200: float | None
    realized_vol_20: float | None
    iv_rank_proxy: float | None
    volume_ratio: float | None
    close_price: float


_REGIME_REQUIRED_KEYS: frozenset[str] = frozenset({"regimes", "close_price"})


# ---------------------------------------------------------------------------
# Forecast blob (shared across models)
# ---------------------------------------------------------------------------

class ForecastShape(TypedDict, total=False):
    symbol: str
    strategy_type: str | None
    as_of_date: str
    horizon_days: int
    trading_days_used: int | None
    analog_count: int
    analogs_used: int | None
    expected_return_low_pct: float
    expected_return_median_pct: float
    expected_return_mean_pct: float
    expected_return_high_pct: float
    positive_outcome_rate_pct: float
    summary: str
    disclaimer: str
    analog_dates: list[str]
    analog_dates_shown: int | None
    analog_dates_total: int | None
    percentile_5: float
    percentile_25: float
    percentile_75: float
    percentile_95: float


_FORECAST_REQUIRED_KEYS: frozenset[str] = frozenset({"horizon_days"})


# ---------------------------------------------------------------------------
# Backtest summary_json (ScannerRecommendation, SweepResult, DailyRecommendation)
# ---------------------------------------------------------------------------

class SummaryShape(TypedDict, total=False):
    trade_count: int
    decided_trades: int
    win_rate: float
    total_roi_pct: float
    max_drawdown_pct: float
    total_net_pnl: float
    total_commissions: float
    starting_equity: float
    ending_equity: float
    average_win_amount: float
    average_loss_amount: float
    average_holding_period_days: float
    average_dte_at_open: float
    sharpe_ratio: float | None
    sortino_ratio: float | None
    profit_factor: float | None
    expectancy: float
    cagr_pct: float | None
    payoff_ratio: float | None
    calmar_ratio: float | None
    max_consecutive_wins: int
    max_consecutive_losses: int
    recovery_factor: float | None


_SUMMARY_REQUIRED_KEYS: frozenset[str] = frozenset({
    "trade_count", "win_rate", "total_roi_pct", "max_drawdown_pct",
    "total_net_pnl", "starting_equity", "ending_equity",
})


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

def validate_json_shape(
    data: Any,
    label: str,
    *,
    required_keys: frozenset[str] = frozenset(),
    known_keys: frozenset[str] | None = None,
    strict: bool = False,
) -> bool:
    """Log a warning if *data* is missing required keys or is the wrong type.

    Returns True when the shape is valid, False otherwise.

    When *strict* is True, a :class:`ValueError` is raised instead of
    returning False so that critical code paths can reject malformed data
    early rather than silently propagating it.

    For multi-leg trade shapes that store per-leg data under a ``legs``
    list, required keys that are absent from the top level are checked
    inside each leg entry before being reported as missing.
    """
    if not isinstance(data, dict):
        logger.warning("json_shape_invalid_type", label=label, got_type=type(data).__name__)
        try:
            from backtestforecast.observability.metrics import JSON_SHAPE_VALIDATION_FAILURES_TOTAL
            JSON_SHAPE_VALIDATION_FAILURES_TOTAL.labels(label=label).inc()
        except Exception:
            pass
        if strict:
            raise ValueError(f"[{label}] expected dict, got {type(data).__name__}")
        return False

    # Partial/in-progress trade records during live position tracking
    # only contain a "phase" key. These are not fully formed trade
    # detail shapes and are intentionally exempt from shape validation.
    if "phase" in data and "entry_date" in data and "legs" not in data:
        return True

    missing = required_keys - data.keys()
    if missing:
        legs = data.get("legs") or data.get("option_legs")
        if isinstance(legs, list) and legs:
            for leg in legs:
                if isinstance(leg, dict):
                    missing -= leg.keys()
                if not missing:
                    break
        if missing:
            logger.warning("json_shape_missing_keys", label=label, missing=sorted(missing))
            try:
                from backtestforecast.observability.metrics import JSON_SHAPE_VALIDATION_FAILURES_TOTAL
                JSON_SHAPE_VALIDATION_FAILURES_TOTAL.labels(label=label).inc()
            except Exception:
                pass
            if strict:
                raise ValueError(f"[{label}] missing required keys: {sorted(missing)}")
            return False

    if known_keys is not None:
        unknown = data.keys() - known_keys - required_keys
        if unknown:
            logger.info("json_shape_unknown_keys", label=label, unknown=sorted(unknown))

    return True
