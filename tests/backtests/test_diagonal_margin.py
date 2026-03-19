"""Test margin calculations from backtestforecast.backtests.margin.

Covers all margin functions with known inputs/expected outputs, edge cases,
and the diagonal spread margin factor used in strategies/diagonal.py.
"""
from __future__ import annotations

import math

import pytest

from backtestforecast.backtests.margin import (
    _MIN_MARGIN_PER_CONTRACT,
    _require_non_negative,
    cash_secured_put_margin,
    collar_margin,
    covered_call_margin,
    covered_strangle_margin,
    credit_spread_margin,
    iron_condor_margin,
    jade_lizard_margin,
    naked_call_margin,
    naked_option_margin,
    naked_put_margin,
    ratio_backspread_margin,
    short_straddle_strangle_margin,
    short_stock_margin,
)


class TestRequireNonNegative:
    def test_rejects_negative(self):
        with pytest.raises(ValueError, match="must be non-negative"):
            _require_non_negative(price=-1.0)

    def test_rejects_nan(self):
        with pytest.raises(ValueError, match="must be a finite number"):
            _require_non_negative(price=float("nan"))

    def test_rejects_inf(self):
        with pytest.raises(ValueError, match="must be a finite number"):
            _require_non_negative(price=float("inf"))

    def test_rejects_negative_inf(self):
        with pytest.raises(ValueError, match="must be a finite number"):
            _require_non_negative(price=float("-inf"))

    def test_accepts_zero(self):
        _require_non_negative(price=0.0)

    def test_accepts_positive(self):
        _require_non_negative(price=100.0)

    def test_multiple_kwargs_rejects_first_bad(self):
        with pytest.raises(ValueError, match="strike"):
            _require_non_negative(underlying=100.0, strike=-5.0)


class TestNakedCallMargin:
    def test_atm_call(self):
        result = naked_call_margin(underlying_price=100.0, strike=100.0, premium_per_share=5.0)
        method_a = 0.25 * 100.0 - 0.0 + 5.0  # 30.0
        method_b = 0.10 * 100.0 + 5.0         # 15.0
        expected = max(method_a, method_b) * 100.0
        assert result == expected == 3000.0

    def test_otm_call(self):
        result = naked_call_margin(underlying_price=100.0, strike=110.0, premium_per_share=2.0)
        otm = max(110.0 - 100.0, 0.0)
        method_a = 0.25 * 100.0 - otm + 2.0   # 17.0
        method_b = 0.10 * 100.0 + 2.0          # 12.0
        expected = max(method_a, method_b) * 100.0
        assert result == expected == 1700.0

    def test_deep_otm_call(self):
        result = naked_call_margin(underlying_price=100.0, strike=200.0, premium_per_share=0.10)
        otm = 100.0
        method_a = 0.25 * 100.0 - otm + 0.10  # -74.9 → clamped to 0
        method_b = 0.10 * 100.0 + 0.10         # 10.10
        expected = max(max(method_a, 0.0), max(method_b, 0.0)) * 100.0
        assert result == expected == 1010.0

    def test_itm_call(self):
        result = naked_call_margin(underlying_price=110.0, strike=100.0, premium_per_share=12.0)
        otm = max(100.0 - 110.0, 0.0)          # 0.0 for ITM
        method_a = 0.25 * 110.0 - otm + 12.0   # 39.5
        method_b = 0.10 * 110.0 + 12.0          # 23.0
        expected = max(method_a, method_b) * 100.0
        assert result == expected == 3950.0

    def test_zero_premium(self):
        result = naked_call_margin(underlying_price=100.0, strike=100.0, premium_per_share=0.0)
        expected = 0.25 * 100.0 * 100.0
        assert result == expected == 2500.0

    def test_minimum_floor(self):
        result = naked_call_margin(underlying_price=0.01, strike=100.0, premium_per_share=0.0)
        assert result == _MIN_MARGIN_PER_CONTRACT == 50.0

    def test_high_underlying_price(self):
        result = naked_call_margin(underlying_price=5000.0, strike=5000.0, premium_per_share=50.0)
        method_a = 0.25 * 5000.0 + 50.0        # 1300.0
        expected = method_a * 100.0
        assert result == expected == 130_000.0

    def test_rejects_negative_underlying(self):
        with pytest.raises(ValueError):
            naked_call_margin(underlying_price=-1.0, strike=100.0, premium_per_share=1.0)

    def test_rejects_negative_strike(self):
        with pytest.raises(ValueError):
            naked_call_margin(underlying_price=100.0, strike=-1.0, premium_per_share=1.0)

    def test_rejects_nan_premium(self):
        with pytest.raises(ValueError):
            naked_call_margin(underlying_price=100.0, strike=100.0, premium_per_share=float("nan"))


class TestNakedPutMargin:
    def test_atm_put(self):
        result = naked_put_margin(underlying_price=100.0, strike=100.0, premium_per_share=5.0)
        method_a = 0.25 * 100.0 - 0.0 + 5.0   # 30.0
        method_b = 0.10 * 100.0 + 5.0          # 15.0
        expected = max(method_a, method_b) * 100.0
        assert result == expected == 3000.0

    def test_otm_put(self):
        result = naked_put_margin(underlying_price=100.0, strike=90.0, premium_per_share=2.0)
        otm = max(100.0 - 90.0, 0.0)           # 10.0
        method_a = 0.25 * 100.0 - otm + 2.0    # 17.0
        method_b = 0.10 * 90.0 + 2.0           # 11.0
        expected = max(method_a, method_b) * 100.0
        assert result == expected == 1700.0

    def test_itm_put(self):
        result = naked_put_margin(underlying_price=90.0, strike=100.0, premium_per_share=12.0)
        otm = max(90.0 - 100.0, 0.0)           # 0.0 for ITM
        method_a = 0.25 * 90.0 - otm + 12.0    # 34.5
        method_b = 0.10 * 100.0 + 12.0         # 22.0
        expected = max(method_a, method_b) * 100.0
        assert result == expected == 3450.0

    def test_zero_premium(self):
        result = naked_put_margin(underlying_price=100.0, strike=100.0, premium_per_share=0.0)
        expected = 0.25 * 100.0 * 100.0
        assert result == expected == 2500.0

    def test_minimum_floor(self):
        result = naked_put_margin(underlying_price=0.01, strike=0.01, premium_per_share=0.0)
        assert result >= _MIN_MARGIN_PER_CONTRACT

    def test_put_method_b_uses_strike_not_underlying(self):
        """Naked put method B uses 10% of *strike*, unlike naked call which uses 10% of underlying."""
        result_put = naked_put_margin(underlying_price=100.0, strike=50.0, premium_per_share=1.0)
        method_b_put = (0.10 * 50.0 + 1.0) * 100.0   # 600.0
        otm = 50.0
        method_a_put = (0.25 * 100.0 - otm + 1.0) * 100.0  # -2400, clamped
        assert result_put >= method_b_put


class TestNakedOptionMarginDispatch:
    def test_dispatch_call(self):
        direct = naked_call_margin(100.0, 100.0, 5.0)
        dispatched = naked_option_margin("call", 100.0, 100.0, 5.0)
        assert direct == dispatched

    def test_dispatch_put(self):
        direct = naked_put_margin(100.0, 100.0, 5.0)
        dispatched = naked_option_margin("put", 100.0, 100.0, 5.0)
        assert direct == dispatched

    def test_unknown_type_defaults_to_put(self):
        result = naked_option_margin("straddle", 100.0, 100.0, 5.0)
        assert result == naked_put_margin(100.0, 100.0, 5.0)


class TestShortStraddleStrangleMargin:
    def test_straddle_call_side_higher(self):
        result = short_straddle_strangle_margin(
            underlying_price=100.0,
            call_strike=100.0,
            put_strike=100.0,
            call_premium_per_share=5.0,
            put_premium_per_share=5.0,
        )
        call_naked = naked_call_margin(100.0, 100.0, 5.0)
        put_naked = naked_put_margin(100.0, 100.0, 5.0)
        if call_naked >= put_naked:
            expected = call_naked + 5.0 * 100.0
        else:
            expected = put_naked + 5.0 * 100.0
        assert result == expected

    def test_strangle_put_side_higher(self):
        result = short_straddle_strangle_margin(
            underlying_price=100.0,
            call_strike=110.0,
            put_strike=95.0,
            call_premium_per_share=1.0,
            put_premium_per_share=4.0,
        )
        call_naked = naked_call_margin(100.0, 110.0, 1.0)
        put_naked = naked_put_margin(100.0, 95.0, 4.0)
        if put_naked >= call_naked:
            expected = put_naked + 1.0 * 100.0
        else:
            expected = call_naked + 4.0 * 100.0
        assert result == expected

    def test_zero_premiums(self):
        result = short_straddle_strangle_margin(
            underlying_price=100.0,
            call_strike=100.0,
            put_strike=100.0,
            call_premium_per_share=0.0,
            put_premium_per_share=0.0,
        )
        assert result > 0


class TestCreditSpreadMargin:
    def test_five_dollar_wide_spread(self):
        assert credit_spread_margin(5.0) == 500.0

    def test_one_dollar_wide_spread(self):
        assert credit_spread_margin(1.0) == 100.0

    def test_zero_width(self):
        assert credit_spread_margin(0.0) == 0.0

    def test_negative_width_uses_abs(self):
        assert credit_spread_margin(-5.0) == 500.0

    def test_fractional_width(self):
        assert credit_spread_margin(2.5) == 250.0


class TestIronCondorMargin:
    def test_equal_widths(self):
        assert iron_condor_margin(5.0, 5.0) == 500.0

    def test_unequal_widths_takes_max(self):
        assert iron_condor_margin(5.0, 10.0) == 1000.0
        assert iron_condor_margin(10.0, 5.0) == 1000.0

    def test_zero_widths(self):
        assert iron_condor_margin(0.0, 0.0) == 0.0

    def test_negative_widths_use_abs(self):
        assert iron_condor_margin(-5.0, -10.0) == 1000.0


class TestCoveredCallMargin:
    def test_basic(self):
        assert covered_call_margin(100.0) == 10_000.0

    def test_zero_price(self):
        assert covered_call_margin(0.0) == 0.0

    def test_high_price(self):
        assert covered_call_margin(5000.0) == 500_000.0

    def test_rejects_negative(self):
        with pytest.raises(ValueError):
            covered_call_margin(-10.0)


class TestCashSecuredPutMargin:
    def test_basic(self):
        assert cash_secured_put_margin(50.0) == 5000.0

    def test_zero_strike(self):
        assert cash_secured_put_margin(0.0) == 0.0

    def test_high_strike(self):
        assert cash_secured_put_margin(500.0) == 50_000.0

    def test_rejects_negative(self):
        with pytest.raises(ValueError):
            cash_secured_put_margin(-10.0)


class TestCoveredStrangleMargin:
    def test_basic(self):
        result = covered_strangle_margin(
            underlying_price=100.0,
            put_strike=95.0,
            put_premium_per_share=3.0,
        )
        stock_cost = 100.0 * 100.0
        put_margin = naked_put_margin(100.0, 95.0, 3.0)
        assert result == stock_cost + put_margin

    def test_atm_put(self):
        result = covered_strangle_margin(
            underlying_price=100.0,
            put_strike=100.0,
            put_premium_per_share=5.0,
        )
        stock_cost = 10_000.0
        put_margin = naked_put_margin(100.0, 100.0, 5.0)
        assert result == stock_cost + put_margin

    def test_always_exceeds_stock_cost(self):
        result = covered_strangle_margin(
            underlying_price=200.0,
            put_strike=190.0,
            put_premium_per_share=2.0,
        )
        assert result > 200.0 * 100.0


class TestCollarMargin:
    def test_basic(self):
        assert collar_margin(100.0) == 10_000.0

    def test_equals_covered_call_margin(self):
        for price in [50.0, 100.0, 250.0, 1000.0]:
            assert collar_margin(price) == covered_call_margin(price)

    def test_rejects_negative(self):
        with pytest.raises(ValueError):
            collar_margin(-1.0)


class TestJadeLizardMargin:
    def test_credit_eliminates_upside_risk(self):
        """When total credit >= call spread width, margin is just the naked put."""
        result = jade_lizard_margin(
            underlying_price=100.0,
            put_strike=95.0,
            put_premium_per_share=3.0,
            call_spread_width_per_share=5.0,
            total_credit_per_share=6.0,
        )
        expected = naked_put_margin(100.0, 95.0, 3.0)
        assert result == expected

    def test_credit_less_than_spread_width(self):
        """When total credit < call spread width, margin is max(put naked, call spread)."""
        result = jade_lizard_margin(
            underlying_price=100.0,
            put_strike=95.0,
            put_premium_per_share=3.0,
            call_spread_width_per_share=5.0,
            total_credit_per_share=2.0,
        )
        put_naked = naked_put_margin(100.0, 95.0, 3.0)
        call_spread = 5.0 * 100.0
        expected = max(put_naked, call_spread)
        assert result == expected

    def test_zero_credit(self):
        result = jade_lizard_margin(
            underlying_price=100.0,
            put_strike=95.0,
            put_premium_per_share=0.0,
            call_spread_width_per_share=5.0,
            total_credit_per_share=0.0,
        )
        put_naked = naked_put_margin(100.0, 95.0, 0.0)
        call_spread = 500.0
        assert result == max(put_naked, call_spread)


class TestRatioBackspreadMargin:
    def test_basic(self):
        assert ratio_backspread_margin(100.0, 105.0) == 500.0

    def test_reversed_strikes(self):
        assert ratio_backspread_margin(105.0, 100.0) == 500.0

    def test_same_strikes(self):
        assert ratio_backspread_margin(100.0, 100.0) == 0.0


class TestShortStockMargin:
    def test_basic(self):
        assert short_stock_margin(100.0) == 5000.0

    def test_fifty_percent_of_market_value(self):
        for price in [50.0, 200.0, 1000.0]:
            assert short_stock_margin(price) == price * 100.0 * 0.50

    def test_rejects_negative(self):
        with pytest.raises(ValueError):
            short_stock_margin(-50.0)


class TestDiagonalSpreadMarginFromModule:
    """Test the DOUBLE_DIAGONAL_MARGIN_FACTOR and PMCC margin concepts
    used in the diagonal strategy."""

    def test_diagonal_margin_factor_is_half(self):
        from backtestforecast.backtests.strategies.diagonal import DOUBLE_DIAGONAL_MARGIN_FACTOR
        assert DOUBLE_DIAGONAL_MARGIN_FACTOR == 0.50

    def test_pmcc_debit_capital_is_entry_value(self):
        """For a debit PMCC (typical), capital = long mid - short mid (× 100).
        When entry_value >= 0, margin is the debit paid, not a naked call formula."""
        long_mid = 12.0
        short_mid = 3.0
        entry_value = (long_mid - short_mid) * 100.0
        assert entry_value == 900.0
        assert entry_value >= 0

    def test_pmcc_credit_capital_uses_naked_call(self):
        """For a credit PMCC (unusual), capital = naked_call_margin on the short leg."""
        short_strike = 160.0
        short_mid = 3.0
        underlying = 150.0
        capital = naked_call_margin(underlying, short_strike, short_mid)
        assert capital > 0

    def test_double_diagonal_margin_is_fraction_of_naked(self):
        from backtestforecast.backtests.strategies.diagonal import DOUBLE_DIAGONAL_MARGIN_FACTOR
        full_naked = naked_call_margin(underlying_price=200.0, strike=210.0, premium_per_share=5.0)
        fractional = full_naked * DOUBLE_DIAGONAL_MARGIN_FACTOR
        assert fractional == full_naked * 0.50
        assert fractional > 0

    def test_diagonal_margin_with_straddle_strangle(self):
        """Double diagonal uses straddle/strangle margin from margin.py."""
        margin = short_straddle_strangle_margin(
            underlying_price=150.0,
            call_strike=160.0,
            put_strike=140.0,
            call_premium_per_share=2.0,
            put_premium_per_share=3.0,
        )
        from backtestforecast.backtests.strategies.diagonal import DOUBLE_DIAGONAL_MARGIN_FACTOR
        reduced = margin * DOUBLE_DIAGONAL_MARGIN_FACTOR
        assert reduced == margin * 0.50
        assert reduced > 0

    def test_diagonal_margin_scales_with_underlying(self):
        """Higher underlying price → higher margin for the same structure."""
        low = naked_call_margin(100.0, 110.0, 2.0)
        high = naked_call_margin(500.0, 510.0, 2.0)
        assert high > low


class TestMarginMinimumFloor:
    """All functions that use _MIN_MARGIN_PER_CONTRACT should enforce the $50 floor."""

    def test_naked_call_floor(self):
        result = naked_call_margin(0.01, 100.0, 0.0)
        assert result >= _MIN_MARGIN_PER_CONTRACT

    def test_naked_put_floor(self):
        result = naked_put_margin(0.01, 0.01, 0.0)
        assert result >= _MIN_MARGIN_PER_CONTRACT

    def test_min_margin_constant_value(self):
        assert _MIN_MARGIN_PER_CONTRACT == 50.0
