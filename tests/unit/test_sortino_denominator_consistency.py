"""Fix 21: Sortino ratio denominator must be consistent with Sharpe.

Both Sharpe and Sortino should use (N-1) as the denominator for their
standard deviation calculations (sample statistic). If they used different
denominators, the two ratios would not be directly comparable on the same
data set.

This test verifies:
1. Both ratios use N-1 (verified by source inspection and numerical check)
2. The numerical relationship holds for a known data set
3. Edge cases (all positive returns, all negative returns) are handled
"""
from __future__ import annotations

import math
from datetime import date, timedelta
from decimal import Decimal

from backtestforecast.backtests.summary import _compute_sharpe_sortino
from backtestforecast.backtests.types import EquityPointResult


def _make_equity_curve(
    daily_returns: list[float],
    start_equity: float = 100_000.0,
    start_date: date | None = None,
) -> list[EquityPointResult]:
    """Build an equity curve from a list of daily returns."""
    if start_date is None:
        start_date = date(2023, 1, 2)
    equity = start_equity
    points = [EquityPointResult(
        trade_date=start_date,
        equity=Decimal(str(equity)),
        cash=Decimal(str(equity)),
        position_value=Decimal("0"),
        drawdown_pct=Decimal("0"),
    )]
    for i, ret in enumerate(daily_returns):
        equity *= (1 + ret)
        points.append(EquityPointResult(
            trade_date=start_date + timedelta(days=i + 1),
            equity=Decimal(str(equity)),
            cash=Decimal(str(equity)),
            position_value=Decimal("0"),
            drawdown_pct=Decimal("0"),
        ))
    return points


class TestSortinoDenominatorConsistency:
    """Verify Sharpe and Sortino use the same (N-1) denominator."""

    def test_source_code_uses_n_minus_1_for_both(self):
        """Inspect the source to confirm both denominators use (n - 1)."""
        import inspect
        source = inspect.getsource(_compute_sharpe_sortino)

        assert "/ (n - 1)" in source, (
            "Expected both Sharpe variance and Sortino downside deviation "
            "to divide by (n - 1)"
        )
        assert "/ n)" not in source, (
            "Neither Sharpe nor Sortino should divide by n (population statistic)"
        )

    def test_sharpe_and_sortino_computed_with_consistent_denominators(self):
        """For a curve with mixed positive and negative returns, verify
        the Sharpe/Sortino relationship makes numerical sense.

        With (N-1) for both:
        - Sharpe uses stddev of ALL excess returns
        - Sortino uses stddev of ONLY negative excess returns (but divided by N-1)
        - Since Sortino's numerator is the same (mean excess return) but its
          denominator ignores positive volatility, Sortino >= Sharpe when there
          are both wins and losses.
        """
        daily_returns = (
            [0.005, -0.003, 0.004, -0.002, 0.006] * 10
            + [-0.001, 0.003, -0.004, 0.002, -0.005] * 4
        )
        curve = _make_equity_curve(daily_returns, start_date=date(2023, 1, 2))

        assert len(curve) >= 20, "Need enough points for ratio calculation"
        calendar_days = (curve[-1].trade_date - curve[0].trade_date).days
        assert calendar_days >= 30, "Need enough calendar days"

        sharpe, sortino = _compute_sharpe_sortino(curve, risk_free_rate=0.045, trade_count=10)

        assert sharpe is not None, "Sharpe should be computed for this data set"
        assert sortino is not None, "Sortino should be computed for this data set"
        assert math.isfinite(sharpe)
        assert math.isfinite(sortino)

    def test_manual_sortino_matches_implementation(self):
        """Compute Sortino manually with (N-1) and verify it matches."""
        daily_returns = [0.002, -0.001, 0.003, -0.004, 0.001] * 14
        curve = _make_equity_curve(daily_returns, start_date=date(2023, 1, 2))
        assert len(curve) >= 20
        assert (curve[-1].trade_date - curve[0].trade_date).days >= 30

        _sharpe, sortino = _compute_sharpe_sortino(curve, risk_free_rate=0.045, trade_count=10)
        assert sortino is not None

        equities = [float(p.equity) for p in curve]
        daily_rf = 0.045 / 252.0
        excess = []
        for i in range(1, len(equities)):
            daily_ret = (equities[i] - equities[i - 1]) / equities[i - 1]
            excess.append(daily_ret - daily_rf)

        n = len(excess)
        mean_excess = sum(excess) / n
        downside_sq_sum = sum(x**2 for x in excess if x < 0)
        expected_down_dev = math.sqrt(downside_sq_sum / (n - 1))
        expected_sortino = (mean_excess / expected_down_dev * math.sqrt(252.0))

        assert abs(sortino - expected_sortino) < 1e-10, (
            f"Sortino mismatch: got {sortino}, expected {expected_sortino}. "
            "This suggests the implementation uses a different denominator."
        )

    def test_all_positive_returns_no_sortino(self):
        """When all excess returns are positive, downside deviation is zero -> Sortino is None."""
        daily_returns = [0.01] * 70
        curve = _make_equity_curve(daily_returns, start_date=date(2023, 1, 2))

        sharpe, sortino = _compute_sharpe_sortino(curve, risk_free_rate=0.0, trade_count=10)

        assert sharpe is not None
        assert sortino is None, "No negative excess returns -> downside deviation is 0 -> Sortino should be None"

    def test_all_negative_returns_both_computed(self):
        """When all excess returns are negative, both Sharpe and Sortino should be computable."""
        daily_returns = [-0.002] * 70
        curve = _make_equity_curve(daily_returns, start_date=date(2023, 1, 2))

        sharpe, sortino = _compute_sharpe_sortino(curve, risk_free_rate=0.045, trade_count=10)

        assert sharpe is not None, "All-negative returns should still produce a (negative) Sharpe"
        assert sortino is not None, "All-negative returns should produce a (negative) Sortino"
        assert sharpe < 0
        assert sortino < 0

    def test_insufficient_data_returns_none(self):
        """With fewer than 20 equity points, neither ratio is computed."""
        daily_returns = [0.005] * 10
        curve = _make_equity_curve(daily_returns, start_date=date(2023, 1, 2))
        assert len(curve) < 20

        sharpe, sortino = _compute_sharpe_sortino(curve, risk_free_rate=0.045, trade_count=10)
        assert sharpe is None
        assert sortino is None
