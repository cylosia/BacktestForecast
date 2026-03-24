from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any


def compute_backtest_score(summary: Mapping[str, Any]) -> float:
    """Return the canonical ranking score for quick/full pipeline results."""
    trade_count = int(summary.get("trade_count", 0) or 0)
    if trade_count <= 0:
        return 0.0
    decided_trades = int(summary.get("decided_trades", trade_count) or 0)

    roi = finite_metric_value(summary.get("total_roi_pct", 0.0))
    raw_win_rate = finite_metric_value(summary.get("win_rate", 0.0))
    win_rate = max(0.0, min(raw_win_rate / 100.0, 1.0))
    drawdown = finite_drawdown_pct(summary.get("max_drawdown_pct"), default=50.0)
    sample_factor = min(decided_trades / 10.0, 1.0)
    sharpe = finite_metric_value(summary.get("sharpe_ratio"))

    score = (
        roi * 0.30
        + win_rate * 100.0 * 0.25
        + sharpe * 20.0 * 0.25
        - drawdown * 0.20
    ) * sample_factor
    if drawdown >= 100.0:
        score = min(score, 0.0)
    return score


def finite_metric_value(value: Any, *, default: float = 0.0) -> float:
    try:
        result = float(default if value is None else value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def finite_drawdown_pct(value: Any, *, default: float = 50.0) -> float:
    return max(0.0, min(finite_metric_value(value, default=default), 100.0))


def apply_support_multiplier(score: float, multiplier: float) -> float:
    """Apply a supportive multiplier without worsening already-negative scores."""
    if multiplier <= 0:
        return score
    if score >= 0:
        return score * multiplier
    return score / multiplier
