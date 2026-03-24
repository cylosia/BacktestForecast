"""Tests for _resolve_position_size Decimal precision."""
from __future__ import annotations

from decimal import Decimal

from backtestforecast.backtests.engine import OptionsBacktestEngine


def test_position_sizing_accepts_decimal_cash():
    result = OptionsBacktestEngine._resolve_position_size(
        available_cash=Decimal("100000.0001"),
        account_size=100000.0,
        risk_per_trade_pct=5.0,
        capital_required_per_unit=1000.0,
        max_loss_per_unit=500.0,
    )
    assert isinstance(result, int)
    assert result > 0


def test_position_sizing_large_account_precision():
    """A $10M account should not lose precision from float truncation."""
    result = OptionsBacktestEngine._resolve_position_size(
        available_cash=Decimal("10000000.50"),
        account_size=10000000.0,
        risk_per_trade_pct=1.0,
        capital_required_per_unit=100000.0,
        max_loss_per_unit=50000.0,
    )
    assert result == 2


def test_position_sizing_zero_cash_per_unit_uses_minimum():
    """When capital_required_per_unit is 0, the $50 minimum kicks in."""
    result = OptionsBacktestEngine._resolve_position_size(
        available_cash=Decimal("50000"),
        account_size=50000.0,
        risk_per_trade_pct=5.0,
        capital_required_per_unit=0.0,
        max_loss_per_unit=None,
        entry_cost_per_unit=0.0,
    )
    assert result > 0


def test_position_sizing_negative_capital():
    result = OptionsBacktestEngine._resolve_position_size(
        available_cash=Decimal("50000"),
        account_size=50000.0,
        risk_per_trade_pct=5.0,
        capital_required_per_unit=-100.0,
        max_loss_per_unit=None,
    )
    assert result == 0
