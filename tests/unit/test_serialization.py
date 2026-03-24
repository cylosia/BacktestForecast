"""Tests for shared serialization helpers."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from backtestforecast.services.serialization import (
    downsample_equity_curve,
    serialize_summary,
    serialize_trade,
)


class TestSerializeSummary:
    def test_nan_becomes_zero(self):
        summary = MagicMock()
        summary.trade_count = 0
        summary.win_rate = float("nan")
        summary.total_roi_pct = 0.0
        summary.average_win_amount = 0.0
        summary.average_loss_amount = 0.0
        summary.average_holding_period_days = 0.0
        summary.average_dte_at_open = 0.0
        summary.max_drawdown_pct = 0.0
        summary.total_commissions = 0.0
        summary.total_net_pnl = 0.0
        summary.starting_equity = 10000.0
        summary.ending_equity = 10000.0
        summary.profit_factor = None
        summary.payoff_ratio = None
        summary.expectancy = 0.0
        summary.sharpe_ratio = None
        summary.sortino_ratio = None
        summary.cagr_pct = None
        summary.calmar_ratio = None
        summary.max_consecutive_wins = 0
        summary.max_consecutive_losses = 0
        summary.recovery_factor = None

        result = serialize_summary(summary)
        assert result["win_rate"] == 0.0

    def test_optional_fields_can_be_none(self):
        summary = MagicMock()
        summary.trade_count = 5
        summary.win_rate = 60.0
        summary.total_roi_pct = 10.0
        summary.average_win_amount = 100.0
        summary.average_loss_amount = 50.0
        summary.average_holding_period_days = 5.0
        summary.average_dte_at_open = 30.0
        summary.max_drawdown_pct = 5.0
        summary.total_commissions = 10.0
        summary.total_net_pnl = 450.0
        summary.starting_equity = 10000.0
        summary.ending_equity = 10450.0
        summary.profit_factor = None
        summary.payoff_ratio = None
        summary.expectancy = 50.0
        summary.sharpe_ratio = None
        summary.sortino_ratio = None
        summary.cagr_pct = None
        summary.calmar_ratio = None
        summary.max_consecutive_wins = 3
        summary.max_consecutive_losses = 1
        summary.recovery_factor = None

        result = serialize_summary(summary)
        assert result["profit_factor"] is None
        assert result["sharpe_ratio"] is None


class TestSerializeTrade:
    def test_trade_serializes_dates(self):
        trade = MagicMock()
        trade.option_ticker = "AAPL260320C00150000"
        trade.strategy_type = "long_call"
        trade.underlying_symbol = "AAPL"
        trade.entry_date = date(2026, 1, 15)
        trade.exit_date = date(2026, 2, 15)
        trade.expiration_date = date(2026, 3, 20)
        trade.quantity = 1
        trade.dte_at_open = 64
        trade.holding_period_days = 31
        trade.entry_underlying_close = 150.0
        trade.exit_underlying_close = 155.0
        trade.entry_mid = 3.5
        trade.exit_mid = 5.0
        trade.gross_pnl = 150.0
        trade.net_pnl = 148.7
        trade.total_commissions = 1.3
        trade.entry_reason = "rsi_below_30"
        trade.exit_reason = "profit_target"
        trade.detail_json = {}

        result = serialize_trade(trade)
        assert result["entry_date"] == "2026-01-15"
        assert result["exit_date"] == "2026-02-15"
        assert result["option_ticker"] == "AAPL260320C00150000"


class TestDownsampleEquityCurve:
    def test_small_curve_not_downsampled(self):
        points = [MagicMock() for _ in range(10)]
        for i, p in enumerate(points):
            p.trade_date = date(2026, 1, i + 1)
            p.equity = 10000.0 + i * 100
            p.cash = 5000.0
            p.position_value = 5000.0 + i * 100
            p.drawdown_pct = 0.0

        result = downsample_equity_curve(points, max_points=500)
        assert len(result) == 10

    def test_large_curve_downsampled(self):
        points = [MagicMock() for _ in range(1000)]
        for i, p in enumerate(points):
            p.trade_date = date(2026, 1, 1)
            p.equity = 10000.0 + i
            p.cash = 5000.0
            p.position_value = 5000.0 + i
            p.drawdown_pct = i * 0.01

        result = downsample_equity_curve(points, max_points=100)
        assert len(result) <= 110
        assert len(result) > 0
