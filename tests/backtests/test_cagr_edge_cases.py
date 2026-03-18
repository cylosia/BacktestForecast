"""Verify CAGR edge cases produce None instead of misleading values."""
from __future__ import annotations

from backtestforecast.backtests.summary import _compute_cagr
from backtestforecast.backtests.types import EquityPointResult
from datetime import date


def _make_curve(start: date, end: date, start_eq: float, end_eq: float) -> list[EquityPointResult]:
    """Create a minimal equity curve with start and end points."""
    points = []
    current = start
    count = 0
    while current <= end and count < 100:
        eq = start_eq + (end_eq - start_eq) * (count / max(99, 1))
        points.append(EquityPointResult(
            trade_date=current,
            equity=eq,
            cash=eq,
            position_value=0.0,
            drawdown_pct=0.0,
        ))
        current = date.fromordinal(current.toordinal() + 1)
        count += 1
    return points


def test_cagr_returns_none_for_zero_ending_equity():
    """When the account reaches zero, CAGR should be None, not -100."""
    curve = _make_curve(date(2024, 1, 1), date(2024, 6, 1), 10000.0, 0.0)
    result = _compute_cagr(10000.0, 0.0, curve)
    assert result is None


def test_cagr_returns_none_for_negative_ending_equity():
    """Negative ending equity (margin scenario) should return None."""
    curve = _make_curve(date(2024, 1, 1), date(2024, 6, 1), 10000.0, -500.0)
    result = _compute_cagr(10000.0, -500.0, curve)
    assert result is None


def test_cagr_returns_none_for_zero_starting_equity():
    curve = _make_curve(date(2024, 1, 1), date(2024, 6, 1), 0.0, 10000.0)
    result = _compute_cagr(0.0, 10000.0, curve)
    assert result is None


def test_cagr_returns_value_for_positive_scenario():
    curve = _make_curve(date(2024, 1, 1), date(2024, 6, 1), 10000.0, 12000.0)
    result = _compute_cagr(10000.0, 12000.0, curve)
    assert result is not None
    assert result > 0
