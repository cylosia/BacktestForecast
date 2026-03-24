"""Test that downsample_equity_curve uses a single pass and preserves max drawdown.

Regression test for the performance issue where the max drawdown index
was found in a separate O(n) scan before the main sampling loop.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from backtestforecast.services.serialization import downsample_equity_curve


@dataclass
class _FakeEquityPoint:
    trade_date: date
    equity: Decimal
    cash: Decimal
    position_value: Decimal
    drawdown_pct: Decimal


def _make_curve(n: int, max_dd_at: int = -1) -> list[_FakeEquityPoint]:
    """Create a fake equity curve of n points with a known max drawdown position."""
    start = date(2024, 1, 1)
    points = []
    for i in range(n):
        dd = Decimal("5.0") if i != max_dd_at else Decimal("25.0")
        points.append(_FakeEquityPoint(
            trade_date=start + timedelta(days=i),
            equity=Decimal("10000") - dd * 100,
            cash=Decimal("10000"),
            position_value=Decimal("0"),
            drawdown_pct=dd,
        ))
    return points


class TestDownsampleEquityCurve:
    def test_small_curve_not_downsampled(self):
        curve = _make_curve(50)
        result = downsample_equity_curve(curve, max_points=500)
        assert len(result) == 50

    def test_large_curve_downsampled(self):
        curve = _make_curve(1000)
        result = downsample_equity_curve(curve, max_points=100)
        assert len(result) <= 110

    def test_max_drawdown_point_always_included(self):
        curve = _make_curve(1000, max_dd_at=503)
        result = downsample_equity_curve(curve, max_points=100)
        max_dd_date = (date(2024, 1, 1) + timedelta(days=503)).isoformat()
        dates = [p["trade_date"] for p in result]
        assert max_dd_date in dates, (
            f"Max drawdown point at index 503 (date {max_dd_date}) must be in the sample"
        )

    def test_first_and_last_always_included(self):
        curve = _make_curve(1000)
        result = downsample_equity_curve(curve, max_points=100)
        assert result[0]["trade_date"] == date(2024, 1, 1).isoformat()
        assert result[-1]["trade_date"] == (date(2024, 1, 1) + timedelta(days=999)).isoformat()

    def test_sample_is_chronologically_sorted(self):
        curve = _make_curve(1000, max_dd_at=777)
        result = downsample_equity_curve(curve, max_points=100)
        dates = [p["trade_date"] for p in result]
        assert dates == sorted(dates), "Downsampled points must be chronologically sorted"

    def test_no_duplicate_dates(self):
        curve = _make_curve(1000, max_dd_at=500)
        result = downsample_equity_curve(curve, max_points=100)
        dates = [p["trade_date"] for p in result]
        assert len(dates) == len(set(dates)), "Downsampled points must not contain duplicate dates"

    def test_max_dd_on_step_boundary_not_duplicated(self):
        curve = _make_curve(1000, max_dd_at=0)
        result = downsample_equity_curve(curve, max_points=100)
        dates = [p["trade_date"] for p in result]
        assert len(dates) == len(set(dates))
