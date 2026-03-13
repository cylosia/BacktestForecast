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


_TRADE_DETAIL_REQUIRED_KEYS: frozenset[str] = frozenset()
_TRADE_DETAIL_KNOWN_KEYS: frozenset[str] = frozenset(TradeDetailShape.__annotations__)


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
    horizon_days: int
    expected_return_median_pct: float
    expected_return_mean_pct: float
    positive_outcome_rate_pct: float
    analog_count: int
    percentile_5: float
    percentile_25: float
    percentile_75: float
    percentile_95: float


_FORECAST_KNOWN_KEYS: frozenset[str] = frozenset(ForecastShape.__annotations__)


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

def validate_json_shape(
    data: Any,
    label: str,
    *,
    required_keys: frozenset[str] = frozenset(),
    known_keys: frozenset[str] | None = None,
) -> bool:
    """Log a warning if *data* is missing required keys or is the wrong type.

    Returns True when the shape is valid, False otherwise.  Never raises.
    """
    if not isinstance(data, dict):
        logger.warning("json_shape_invalid_type", label=label, got_type=type(data).__name__)
        return False

    missing = required_keys - data.keys()
    if missing:
        logger.warning("json_shape_missing_keys", label=label, missing=sorted(missing))
        return False

    return True
