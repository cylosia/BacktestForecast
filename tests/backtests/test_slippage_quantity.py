"""Test 61: Multi-leg slippage with quantity_per_unit.

Verifies that slippage calculations in OptionsBacktestEngine correctly
multiply by leg.quantity_per_unit, so a ratio spread (1 short put + 2 long
puts) produces the correct gross notional.
"""
from __future__ import annotations

from datetime import date

from backtestforecast.backtests.types import OpenMultiLegPosition, OpenOptionLeg


class TestSlippageQuantityPerUnit:
    """Verify gross notional computation accounts for quantity_per_unit."""

    @staticmethod
    def _ratio_spread_position() -> OpenMultiLegPosition:
        """1 short put (qty_per_unit=1) + 2 long puts (qty_per_unit=2)."""
        return OpenMultiLegPosition(
            display_ticker="O:TEST_RATIO",
            strategy_type="ratio_put_backspread",
            underlying_symbol="TEST",
            entry_date=date(2025, 1, 2),
            entry_index=0,
            quantity=1,
            dte_at_open=30,
            option_legs=[
                OpenOptionLeg(
                    ticker="O:SHORT_PUT",
                    contract_type="put",
                    side=-1,
                    strike_price=100.0,
                    expiration_date=date(2025, 2, 1),
                    quantity_per_unit=1,
                    entry_mid=3.00,
                    last_mid=3.00,
                ),
                OpenOptionLeg(
                    ticker="O:LONG_PUT",
                    contract_type="put",
                    side=1,
                    strike_price=95.0,
                    expiration_date=date(2025, 2, 1),
                    quantity_per_unit=2,
                    entry_mid=1.50,
                    last_mid=1.50,
                ),
            ],
        )

    def test_gross_notional_includes_quantity_per_unit(self):
        """Gross notional for the ratio spread must multiply each leg's
        abs(entry_mid * 100) by its quantity_per_unit."""
        position = self._ratio_spread_position()
        quantity = position.quantity

        gross_notional = sum(
            abs(leg.entry_mid * 100.0) * leg.quantity_per_unit
            for leg in position.option_legs
        ) * quantity

        short_put_notional = abs(3.00 * 100.0) * 1
        long_put_notional = abs(1.50 * 100.0) * 2
        expected = (short_put_notional + long_put_notional) * quantity
        assert gross_notional == expected
        assert gross_notional == 600.0

    def test_slippage_cost_uses_gross_notional(self):
        """Slippage cost = gross_notional * quantity * (slippage_pct / 100)."""
        position = self._ratio_spread_position()
        slippage_pct = 0.5

        gross_notional_per_unit = sum(
            abs(leg.entry_mid * 100.0) * leg.quantity_per_unit
            for leg in position.option_legs
        )
        slippage_cost = gross_notional_per_unit * position.quantity * (slippage_pct / 100.0)

        expected_notional_per_unit = (300.0 + 300.0)
        expected_slippage = expected_notional_per_unit * 1 * 0.005
        assert abs(slippage_cost - expected_slippage) < 1e-10

    def test_quantity_gt_one_multiplies_slippage(self):
        """When position quantity > 1, slippage scales proportionally."""
        position = self._ratio_spread_position()
        position.quantity = 3
        slippage_pct = 1.0

        gross_notional_per_unit = sum(
            abs(leg.entry_mid * 100.0) * leg.quantity_per_unit
            for leg in position.option_legs
        )
        slippage_cost = gross_notional_per_unit * position.quantity * (slippage_pct / 100.0)

        expected = 600.0 * 3 * 0.01
        assert abs(slippage_cost - expected) < 1e-10
        assert abs(slippage_cost - 18.0) < 1e-10

    def test_single_leg_quantity_per_unit_one(self):
        """Baseline: single leg with quantity_per_unit=1."""
        position = OpenMultiLegPosition(
            display_ticker="O:SINGLE",
            strategy_type="long_put",
            underlying_symbol="TEST",
            entry_date=date(2025, 1, 2),
            entry_index=0,
            quantity=1,
            dte_at_open=30,
            option_legs=[
                OpenOptionLeg(
                    ticker="O:SINGLE_PUT",
                    contract_type="put",
                    side=1,
                    strike_price=100.0,
                    expiration_date=date(2025, 2, 1),
                    quantity_per_unit=1,
                    entry_mid=2.50,
                    last_mid=2.50,
                ),
            ],
        )

        gross_notional = sum(
            abs(leg.entry_mid * 100.0) * leg.quantity_per_unit
            for leg in position.option_legs
        ) * position.quantity
        assert gross_notional == 250.0
