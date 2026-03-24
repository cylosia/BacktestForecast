from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def compute_backtest_score(summary: Mapping[str, Any]) -> float:
    """Return the canonical ranking score for quick/full pipeline results."""
    trade_count = int(summary.get("trade_count", 0) or 0)
    if trade_count <= 0:
        return 0.0

    roi = float(summary.get("total_roi_pct", 0.0) or 0.0)
    raw_win_rate = float(summary.get("win_rate", 0.0) or 0.0)
    win_rate = max(0.0, min(raw_win_rate / 100.0, 1.0))
    drawdown = min(float(summary.get("max_drawdown_pct", 50.0) or 0.0), 100.0)
    sample_factor = min(trade_count / 10.0, 1.0)
    sharpe = float(summary.get("sharpe_ratio") or 0.0)

    score = (
        roi * 0.30
        + win_rate * 100.0 * 0.25
        + sharpe * 20.0 * 0.25
        - drawdown * 0.20
    ) * sample_factor
    if drawdown >= 100.0:
        score = min(score, 0.0)
    return score
