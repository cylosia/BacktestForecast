"""Fix 77: NaN values in trade detail JSON must be sanitized to None.

Tests that _build_trade_detail_json replaces NaN capital_required_per_unit
with None instead of propagating NaN into the JSON output.
"""
from __future__ import annotations

import math
from datetime import date

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.types import OpenMultiLegPosition, OpenOptionLeg


def _make_position(
    capital_required_per_unit: float = 500.0,
    max_loss_per_unit: float | None = None,
    max_profit_per_unit: float | None = None,
    quantity: int = 1,
) -> OpenMultiLegPosition:
    """Create a minimal position for testing _build_trade_detail_json."""
    leg = OpenOptionLeg(
        ticker="AAPL230120C00150000",
        contract_type="call",
        side=1,
        strike_price=150.0,
        expiration_date=date(2023, 1, 20),
        quantity_per_unit=1,
        entry_mid=3.0,
        last_mid=3.5,
    )
    return OpenMultiLegPosition(
        display_ticker="AAPL230120C00150000",
        strategy_type="long_call",
        underlying_symbol="AAPL",
        entry_date=date(2023, 1, 3),
        entry_index=0,
        quantity=quantity,
        dte_at_open=17,
        option_legs=[leg],
        capital_required_per_unit=capital_required_per_unit,
        max_loss_per_unit=max_loss_per_unit,
        max_profit_per_unit=max_profit_per_unit,
        detail_json={"legs": [{"ticker": leg.ticker, "entry_mid": leg.entry_mid}]},
    )


class TestNaNPropagation:
    """Verify NaN is sanitized to None in trade detail JSON."""

    def test_nan_capital_required_becomes_none(self):
        """NaN capital_required_per_unit × quantity should produce None."""
        position = _make_position(capital_required_per_unit=float("nan"))
        exit_prices = {"AAPL230120C00150000": 3.5}
        detail = OptionsBacktestEngine._build_trade_detail_json(
            position, exit_prices, exit_value_per_unit=350.0,
        )
        assert detail["capital_required_total"] is None, (
            "NaN capital_required_per_unit should result in None, not NaN"
        )

    def test_finite_capital_required_preserved(self):
        """Normal finite values should pass through unchanged."""
        position = _make_position(capital_required_per_unit=500.0)
        exit_prices = {"AAPL230120C00150000": 3.5}
        detail = OptionsBacktestEngine._build_trade_detail_json(
            position, exit_prices, exit_value_per_unit=350.0,
        )
        assert detail["capital_required_total"] == 500.0

    def test_nan_max_loss_becomes_none(self):
        """NaN max_loss_per_unit × quantity should produce None."""
        position = _make_position(max_loss_per_unit=float("nan"))
        exit_prices = {"AAPL230120C00150000": 3.5}
        detail = OptionsBacktestEngine._build_trade_detail_json(
            position, exit_prices, exit_value_per_unit=350.0,
        )
        assert detail["max_loss_total"] is None

    def test_nan_max_profit_becomes_none(self):
        """NaN max_profit_per_unit × quantity should produce None."""
        position = _make_position(max_profit_per_unit=float("nan"))
        exit_prices = {"AAPL230120C00150000": 3.5}
        detail = OptionsBacktestEngine._build_trade_detail_json(
            position, exit_prices, exit_value_per_unit=350.0,
        )
        assert detail["max_profit_total"] is None

    def test_none_max_loss_stays_none(self):
        """None max_loss_per_unit should remain None (not crash)."""
        position = _make_position(max_loss_per_unit=None)
        exit_prices = {"AAPL230120C00150000": 3.5}
        detail = OptionsBacktestEngine._build_trade_detail_json(
            position, exit_prices, exit_value_per_unit=350.0,
        )
        assert detail["max_loss_total"] is None

    def test_inf_capital_required_becomes_none(self):
        """Infinity should also be sanitized to None."""
        position = _make_position(capital_required_per_unit=float("inf"))
        exit_prices = {"AAPL230120C00150000": 3.5}
        detail = OptionsBacktestEngine._build_trade_detail_json(
            position, exit_prices, exit_value_per_unit=350.0,
        )
        assert detail["capital_required_total"] is None
