"""Tests for _entry_underlying_close with missing data."""
from __future__ import annotations

from unittest.mock import MagicMock

from backtestforecast.backtests.engine import OptionsBacktestEngine


def test_returns_stock_leg_price_when_available():
    position = MagicMock()
    stock_leg = MagicMock()
    stock_leg.entry_price = 150.25
    position.stock_legs = [stock_leg]
    assert OptionsBacktestEngine._entry_underlying_close(position) == 150.25


def test_returns_detail_json_value_when_no_stock_legs():
    position = MagicMock()
    position.stock_legs = []
    position.option_legs = [MagicMock()]
    position.detail_json = {"entry_underlying_close": 200.50}
    assert OptionsBacktestEngine._entry_underlying_close(position) == 200.50


def test_returns_zero_with_warning_when_missing():
    position = MagicMock()
    position.stock_legs = []
    position.option_legs = [MagicMock()]
    position.detail_json = {}
    position.display_ticker = "TEST"
    result = OptionsBacktestEngine._entry_underlying_close(position)
    assert result == 0.0


def test_returns_zero_for_none_value():
    position = MagicMock()
    position.stock_legs = []
    position.option_legs = []
    position.detail_json = {"entry_underlying_close": None}
    result = OptionsBacktestEngine._entry_underlying_close(position)
    assert result == 0.0
