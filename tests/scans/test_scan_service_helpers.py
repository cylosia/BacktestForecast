from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from backtestforecast.services.scan_service_helpers import (
    historical_metric_or_none,
    historical_observation_from_summary,
)


def test_historical_metric_or_none_accepts_float_json_values() -> None:
    assert historical_metric_or_none({"win_rate": 58.33}, "win_rate") == 58.33
    assert historical_metric_or_none({"win_rate": Decimal("58.33")}, "win_rate") == 58.33
    assert historical_metric_or_none({"win_rate": "not-a-number"}, "win_rate") is None
    assert historical_metric_or_none({"win_rate": float("inf")}, "win_rate") is None


def test_historical_observation_from_summary_accepts_float_summary_payloads() -> None:
    observation = historical_observation_from_summary(
        completed_at=datetime(2026, 4, 1, tzinfo=UTC),
        summary={
            "win_rate": 62.5,
            "total_roi_pct": Decimal("14.25"),
            "max_drawdown_pct": 8.75,
        },
    )

    assert observation is not None
    assert observation.win_rate == 62.5
    assert observation.total_roi_pct == 14.25
    assert observation.max_drawdown_pct == 8.75
