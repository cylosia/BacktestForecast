"""Test 79: Verify _downsample_equity_curve reduces 501-999 point curves
to â‰¤ 500 points using ceiling division.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from backtestforecast.services.scans import ScanService


@dataclass
class _FakeEquityPoint:
    trade_date: date
    equity: float
    cash: float
    position_value: float
    drawdown_pct: float


def _make_curve(n: int) -> list[_FakeEquityPoint]:
    return [
        _FakeEquityPoint(
            trade_date=date(2024, 1, 1) + timedelta(days=i),
            equity=10000.0 + i,
            cash=10000.0 + i,
            position_value=0.0,
            drawdown_pct=0.0,
        )
        for i in range(n)
    ]


class TestDownsampleEquityCurve:
    def test_500_or_fewer_points_not_downsampled(self):
        curve = _make_curve(500)
        result = ScanService._downsample_equity_curve(curve)
        assert len(result) == 500

    def test_501_points_reduced_to_at_most_500(self):
        curve = _make_curve(501)
        result = ScanService._downsample_equity_curve(curve)
        assert len(result) <= 500

    def test_750_points_reduced_to_at_most_500(self):
        curve = _make_curve(750)
        result = ScanService._downsample_equity_curve(curve)
        assert len(result) <= 500

    def test_999_points_reduced_to_at_most_501(self):
        curve = _make_curve(999)
        result = ScanService._downsample_equity_curve(curve)
        assert len(result) <= 501

    def test_1000_points_reduced_to_at_most_501(self):
        curve = _make_curve(1000)
        result = ScanService._downsample_equity_curve(curve)
        assert len(result) <= 501

    def test_first_and_last_points_always_included(self):
        curve = _make_curve(600)
        result = ScanService._downsample_equity_curve(curve)
        assert result[0]["trade_date"] == curve[0].trade_date.isoformat()
        assert result[-1]["trade_date"] == curve[-1].trade_date.isoformat()

    def test_output_has_expected_keys(self):
        curve = _make_curve(501)
        result = ScanService._downsample_equity_curve(curve)
        for point in result:
            assert "trade_date" in point
            assert "equity" in point

    def test_empty_curve(self):
        result = ScanService._downsample_equity_curve([])
        assert result == []

    def test_single_point_curve(self):
        curve = _make_curve(1)
        result = ScanService._downsample_equity_curve(curve)
        assert len(result) == 1
