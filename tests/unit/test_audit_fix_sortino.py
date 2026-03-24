"""Verify Sortino ratio uses sample standard deviation (N-1 denominator)."""
from __future__ import annotations

import math
from datetime import date, timedelta

from backtestforecast.backtests.summary import _compute_sharpe_sortino
from backtestforecast.backtests.types import EquityPointResult, TradeResult


def _equity_curve(equities: list[float], start: date | None = None) -> list[EquityPointResult]:
    base = start or date(2024, 1, 2)
    peak = equities[0]
    result = []
    for i, eq in enumerate(equities):
        peak = max(peak, eq)
        dd = ((peak - eq) / peak * 100.0) if peak > 0 else 0.0
        result.append(EquityPointResult(
            trade_date=base + timedelta(days=i),
            equity=eq,
            cash=eq,
            position_value=0.0,
            drawdown_pct=dd,
        ))
    return result


def _trade(net_pnl: float, *, day_offset: int = 0) -> TradeResult:
    base = date(2024, 1, 2) + timedelta(days=day_offset)
    return TradeResult(
        option_ticker=f"T{day_offset}",
        strategy_type="long_call",
        underlying_symbol="TEST",
        entry_date=base,
        exit_date=base + timedelta(days=5),
        expiration_date=base + timedelta(days=30),
        quantity=1,
        dte_at_open=30,
        holding_period_days=5,
        entry_underlying_close=100.0,
        exit_underlying_close=105.0,
        entry_mid=2.0,
        exit_mid=3.0,
        gross_pnl=net_pnl + 1.0,
        net_pnl=net_pnl,
        total_commissions=1.0,
        entry_reason="signal",
        exit_reason="expiration",
        detail_json={},
    )


class TestSortinoDenominator:
    """The Sortino ratio must use N-1 (sample) denominator, matching Sharpe."""

    def test_sortino_uses_sample_denominator(self):
        equities = [10000.0]
        for i in range(60):
            change = 50.0 if i % 3 != 0 else -80.0
            equities.append(equities[-1] + change)
        curve = _equity_curve(equities)
        trades = [_trade(50.0 if i % 3 != 0 else -80.0, day_offset=i * 7) for i in range(20)]

        sharpe, sortino = _compute_sharpe_sortino(curve, 0.045, len(trades))
        assert sharpe is not None
        assert sortino is not None

        # Manually verify denominator
        daily_rf = 0.045 / 252.0
        excess = []
        for i in range(1, len(equities)):
            daily_ret = (equities[i] - equities[i - 1]) / equities[i - 1]
            excess.append(daily_ret - daily_rf)

        n = len(excess)
        mean_excess = sum(excess) / n

        # Sharpe: sample std (N-1)
        variance = sum((x - mean_excess) ** 2 for x in excess) / (n - 1)
        expected_sharpe = (mean_excess / math.sqrt(variance)) * math.sqrt(252.0) if variance > 0 else None

        # Sortino: sample downside deviation (N-1) - THIS IS THE FIX
        downside_sq = sum(x ** 2 for x in excess if x < 0)
        expected_sortino = (mean_excess / math.sqrt(downside_sq / (n - 1))) * math.sqrt(252.0)

        assert abs(sharpe - expected_sharpe) < 1e-10, f"Sharpe mismatch: {sharpe} vs {expected_sharpe}"
        assert abs(sortino - expected_sortino) < 1e-10, f"Sortino mismatch: {sortino} vs {expected_sortino}"

    def test_sortino_returns_none_for_insufficient_data(self):
        equities = [10000.0, 10050.0]
        curve = _equity_curve(equities)
        sharpe, sortino = _compute_sharpe_sortino(curve, 0.045, 1)
        assert sharpe is None
        assert sortino is None
