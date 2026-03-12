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
