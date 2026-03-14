"""Unit tests for _resolve_position_size."""
from __future__ import annotations

import pytest

from backtestforecast.backtests.engine import OptionsBacktestEngine


class TestResolvePositionSize:
    @staticmethod
    def _call(**kwargs):
        return OptionsBacktestEngine._resolve_position_size(**kwargs)

    def test_basic_sizing(self):
        result = self._call(
            available_cash=10_000,
            account_size=10_000,
            risk_per_trade_pct=5,
            capital_required_per_unit=500,
            max_loss_per_unit=200,
        )
        assert result == 2

    def test_cash_limited(self):
        result = self._call(
            available_cash=400,
            account_size=10_000,
            risk_per_trade_pct=10,
            capital_required_per_unit=500,
            max_loss_per_unit=100,
        )
        assert result == 0

    def test_zero_capital_required(self):
        result = self._call(
            available_cash=10_000,
            account_size=10_000,
            risk_per_trade_pct=5,
            capital_required_per_unit=0,
            max_loss_per_unit=100,
        )
        assert result == 0

    def test_none_max_loss_uses_capital(self):
        result = self._call(
            available_cash=10_000,
            account_size=10_000,
            risk_per_trade_pct=5,
            capital_required_per_unit=500,
            max_loss_per_unit=None,
        )
        assert result == 1

    def test_commission_reduces_units(self):
        without_commission = self._call(
            available_cash=10_000,
            account_size=10_000,
            risk_per_trade_pct=10,
            capital_required_per_unit=1_000,
            max_loss_per_unit=500,
            commission_per_unit=0,
        )
        with_commission = self._call(
            available_cash=10_000,
            account_size=10_000,
            risk_per_trade_pct=10,
            capital_required_per_unit=1_000,
            max_loss_per_unit=500,
            commission_per_unit=500,
        )
        assert with_commission <= without_commission

    def test_negative_max_loss(self):
        result = self._call(
            available_cash=10_000,
            account_size=10_000,
            risk_per_trade_pct=5,
            capital_required_per_unit=500,
            max_loss_per_unit=-100,
        )
        assert result >= 0


class TestCoveredStranglePositionSizingWithMaxLoss:
    """Item 73: Verify that covered strangle now has a non-None max_loss
    and the position sizer produces a bounded position size."""

    @staticmethod
    def _call(**kwargs):
        return OptionsBacktestEngine._resolve_position_size(**kwargs)

    def test_max_loss_is_non_none(self):
        """Covered strangle margin + naked put creates a finite max_loss,
        which should be non-None and positive."""
        from backtestforecast.backtests.margin import covered_strangle_margin, naked_put_margin

        spot = 100.0
        put_strike = 90.0
        put_premium = 2.0
        call_premium = 3.0

        capital = covered_strangle_margin(spot, put_strike, put_premium)
        credit = (call_premium + put_premium) * 100.0
        naked_put_req = naked_put_margin(spot, put_strike, put_premium)
        max_loss = naked_put_req + spot * 100.0 - credit

        assert max_loss is not None
        assert max_loss > 0

    def test_bounded_position_size_with_max_loss(self):
        """When max_loss is set (non-None), position size should be bounded
        by the risk budget and never exceed what cash allows."""
        from backtestforecast.backtests.margin import covered_strangle_margin, naked_put_margin

        spot = 100.0
        put_strike = 90.0
        put_premium = 2.0
        call_premium = 3.0

        capital = covered_strangle_margin(spot, put_strike, put_premium)
        credit = (call_premium + put_premium) * 100.0
        naked_put_req = naked_put_margin(spot, put_strike, put_premium)
        max_loss = naked_put_req + spot * 100.0 - credit

        result = self._call(
            available_cash=500_000,
            account_size=500_000,
            risk_per_trade_pct=10,
            capital_required_per_unit=capital,
            max_loss_per_unit=max_loss,
        )
        assert result >= 1
        risk_budget = 500_000 * 0.10
        assert result <= risk_budget / max_loss + 1

    def test_max_loss_none_vs_set_gives_different_sizes(self):
        """Position size with max_loss=None (uses capital) should differ
        from when max_loss is explicitly set to a smaller value."""
        capital = 11_200.0
        max_loss = 10_700.0

        size_with_max_loss = self._call(
            available_cash=100_000,
            account_size=100_000,
            risk_per_trade_pct=10,
            capital_required_per_unit=capital,
            max_loss_per_unit=max_loss,
        )
        size_without_max_loss = self._call(
            available_cash=100_000,
            account_size=100_000,
            risk_per_trade_pct=10,
            capital_required_per_unit=capital,
            max_loss_per_unit=None,
        )
        assert size_with_max_loss >= size_without_max_loss
