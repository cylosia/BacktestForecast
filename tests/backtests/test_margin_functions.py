"""Comprehensive tests for all margin calculation functions.

Reference: CBOE Margin Manual, FINRA Rule 4210.
All formulas verified against src/backtestforecast/backtests/margin.py.
"""

from __future__ import annotations

import pytest

from backtestforecast.backtests.margin import (
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
    short_stock_margin,
    short_straddle_strangle_margin,
)


# ---------------------------------------------------------------------------
# naked_call_margin
# ---------------------------------------------------------------------------


class TestNakedCallMargin:
    """Tests for naked_call_margin(underlying_price, strike, premium_per_share)."""

    def test_standard_otm_call(self):
        """OTM call: underlying=100, strike=110, premium=2.00."""
        # OTM = 10; method_a = 0.25*100 - 10 + 2 = 17; method_b = 0.10*100 + 2 = 12
        # per_share = 17; result = 1700
        result = naked_call_margin(100.0, 110.0, 2.0)
        assert result == 1700.0

    def test_atm_call(self):
        """ATM call: underlying=100, strike=100, premium=2.00."""
        # OTM = 0; method_a = 25 + 2 = 27; method_b = 10 + 2 = 12
        result = naked_call_margin(100.0, 100.0, 2.0)
        assert result == 2700.0

    def test_deep_itm_call(self):
        """Deep ITM call: underlying=100, strike=80, premium=22.00."""
        # OTM = 0; method_a = 25 + 22 = 47; method_b = 10 + 22 = 32
        result = naked_call_margin(100.0, 80.0, 22.0)
        assert result == 4700.0

    def test_deep_otm_call(self):
        """Deep OTM call: underlying=100, strike=150, premium=0.50."""
        # OTM = 50; method_a = 25 - 50 + 0.50 = -24.5 -> 0; method_b = 10 + 0.50 = 10.5
        result = naked_call_margin(100.0, 150.0, 0.50)
        assert result == 1050.0

    def test_minimum_floor_50(self):
        """Result must be at least $50 per contract."""
        # Very small underlying: 0.25*5 - 0 + 0 = 1.25; 1.25*100 = 125
        # Even smaller: underlying=0.40 -> 0.25*0.4 = 0.1; 0.1*100 = 10 -> floor 50
        result = naked_call_margin(0.40, 0.40, 0.0)
        assert result == 50.0

    def test_zero_premium(self):
        """Zero premium: underlying=100, strike=110, premium=0."""
        result = naked_call_margin(100.0, 110.0, 0.0)
        assert result == 1500.0  # 0.25*100 - 10 + 0 = 15 per share

    def test_always_positive(self):
        """Result must always be positive."""
        for u, s, p in [(100, 110, 2), (100, 100, 0), (50, 200, 0.01)]:
            assert naked_call_margin(u, s, p) > 0

    def test_negative_underlying_raises(self):
        with pytest.raises(ValueError, match="underlying_price must be non-negative"):
            naked_call_margin(-1.0, 100.0, 2.0)


# ---------------------------------------------------------------------------
# naked_put_margin
# ---------------------------------------------------------------------------


class TestNakedPutMargin:
    """Tests for naked_put_margin(underlying_price, strike, premium_per_share)."""

    def test_standard_otm_put(self):
        """OTM put: underlying=100, strike=90, premium=2.00."""
        # OTM = 10; method_a = 0.25*100 - 10 + 2 = 17; method_b = 0.10*90 + 2 = 11
        result = naked_put_margin(100.0, 90.0, 2.0)
        assert result == 1700.0

    def test_atm_put(self):
        """ATM put: underlying=100, strike=100, premium=2.00."""
        # OTM = 0; method_a = 25 + 2 = 27; method_b = 10 + 2 = 12
        result = naked_put_margin(100.0, 100.0, 2.0)
        assert result == 2700.0

    def test_deep_itm_put(self):
        """Deep ITM put: underlying=100, strike=120, premium=22.00."""
        # OTM = 0; method_a = 25 + 22 = 47; method_b = 0.10*120 + 22 = 34
        result = naked_put_margin(100.0, 120.0, 22.0)
        assert result == 4700.0

    def test_deep_otm_put(self):
        """Deep OTM put: underlying=100, strike=50, premium=0.50."""
        # OTM = 50; method_a = 25 - 50 + 0.50 = -24.5 -> 0; method_b = 0.10*50 + 0.50 = 5.5
        result = naked_put_margin(100.0, 50.0, 0.50)
        assert result == 550.0

    def test_minimum_floor_50(self):
        """Result must be at least $50 per contract."""
        result = naked_put_margin(0.40, 0.40, 0.0)
        assert result == 50.0

    def test_zero_premium(self):
        """Zero premium: underlying=100, strike=90, premium=0."""
        result = naked_put_margin(100.0, 90.0, 0.0)
        assert result == 1500.0

    def test_always_positive(self):
        """Result must always be positive."""
        for u, s, p in [(100, 90, 2), (100, 100, 0), (50, 10, 0.01)]:
            assert naked_put_margin(u, s, p) > 0


# ---------------------------------------------------------------------------
# naked_option_margin
# ---------------------------------------------------------------------------


class TestNakedOptionMargin:
    """Tests for naked_option_margin(contract_type, underlying_price, strike, premium_per_share)."""

    def test_dispatches_to_call(self):
        """Call type uses naked_call_margin."""
        call_result = naked_option_margin("call", 100.0, 110.0, 2.0)
        expected = naked_call_margin(100.0, 110.0, 2.0)
        assert call_result == expected == 1700.0

    def test_dispatches_to_put(self):
        """Put type uses naked_put_margin."""
        put_result = naked_option_margin("put", 100.0, 90.0, 2.0)
        expected = naked_put_margin(100.0, 90.0, 2.0)
        assert put_result == expected == 1700.0

    def test_put_for_non_call(self):
        """Any non-call string dispatches to put (e.g. 'put', 'PUT')."""
        assert naked_option_margin("put", 100.0, 90.0, 2.0) == naked_put_margin(100.0, 90.0, 2.0)
        assert naked_option_margin("PUT", 100.0, 90.0, 2.0) == naked_put_margin(100.0, 90.0, 2.0)


# ---------------------------------------------------------------------------
# credit_spread_margin
# ---------------------------------------------------------------------------


class TestCreditSpreadMargin:
    """Tests for credit_spread_margin(spread_width_per_share)."""

    def test_standard_spread(self):
        """$5 wide spread -> $500 margin."""
        result = credit_spread_margin(5.0)
        assert result == 500.0

    def test_narrow_spread(self):
        """$1 wide spread -> $100 margin."""
        result = credit_spread_margin(1.0)
        assert result == 100.0

    def test_negative_width_uses_abs(self):
        """Negative width is converted to absolute value."""
        result = credit_spread_margin(-5.0)
        assert result == 500.0

    def test_zero_width(self):
        """Zero width -> $0 margin."""
        result = credit_spread_margin(0.0)
        assert result == 0.0

    def test_always_non_negative(self):
        """Result must be non-negative."""
        for w in [-10.0, -1.0, 0.0, 1.0, 10.0]:
            assert credit_spread_margin(w) >= 0


# ---------------------------------------------------------------------------
# iron_condor_margin
# ---------------------------------------------------------------------------


class TestIronCondorMargin:
    """Tests for iron_condor_margin(call_spread_width, put_spread_width)."""

    def test_equal_widths(self):
        """Equal widths: both $5 -> $500 margin."""
        result = iron_condor_margin(5.0, 5.0)
        assert result == 500.0

    def test_call_side_greater(self):
        """Call spread wider: call $10, put $5 -> $1000."""
        result = iron_condor_margin(10.0, 5.0)
        assert result == 1000.0

    def test_put_side_greater(self):
        """Put spread wider: call $5, put $10 -> $1000."""
        result = iron_condor_margin(5.0, 10.0)
        assert result == 1000.0

    def test_negative_widths_use_abs(self):
        """Negative widths use absolute value."""
        result = iron_condor_margin(-5.0, -10.0)
        assert result == 1000.0

    def test_zero_widths(self):
        """Both zero -> $0 margin."""
        result = iron_condor_margin(0.0, 0.0)
        assert result == 0.0

    def test_always_non_negative(self):
        """Result must be non-negative."""
        for c, p in [(-5, 5), (5, -5), (0, 0), (10, 3)]:
            assert iron_condor_margin(c, p) >= 0


# ---------------------------------------------------------------------------
# short_straddle_strangle_margin
# ---------------------------------------------------------------------------


class TestShortStraddleStrangleMargin:
    """Tests for short_straddle_strangle_margin(underlying, call_strike, put_strike, call_premium, put_premium)."""

    def test_straddle_atm_call_greater(self):
        """ATM straddle where call naked >= put naked; add put premium."""
        # Both ATM: call_naked = put_naked = 2700. Call >= put, so add put_premium*100
        # put_premium=2 -> add 200; result = 2700 + 200 = 2900
        result = short_straddle_strangle_margin(100.0, 100.0, 100.0, 2.0, 2.0)
        assert result == 2900.0

    def test_strangle_put_side_greater(self):
        """Strangle where put side has greater naked margin."""
        # OTM call 110: naked_call = 1700; OTM put 90: naked_put = 1700
        # Equal, so call >= put -> add put premium. 1700 + 2*100 = 1900
        result = short_straddle_strangle_margin(100.0, 110.0, 90.0, 2.0, 2.0)
        assert result == 1900.0

    def test_call_side_greater_adds_put_premium(self):
        """When call naked > put naked, add put premium."""
        # Call ITM (strike 80): naked_call = 4700; Put OTM (strike 90): naked_put = 1700
        result = short_straddle_strangle_margin(100.0, 80.0, 90.0, 22.0, 2.0)
        assert result == 4700.0 + 2.0 * 100.0  # 4900

    def test_put_side_greater_adds_call_premium(self):
        """When put naked > call naked, add call premium."""
        # Put ITM (strike 120): naked_put = 4700; Call OTM (strike 110): naked_call = 1700
        result = short_straddle_strangle_margin(100.0, 110.0, 120.0, 2.0, 22.0)
        assert result == 4700.0 + 2.0 * 100.0  # 4900

    def test_zero_premiums(self):
        """Zero premiums on both sides."""
        result = short_straddle_strangle_margin(100.0, 100.0, 100.0, 0.0, 0.0)
        assert result == 2700.0  # naked only, no premium add

    def test_always_positive(self):
        """Result must always be positive."""
        result = short_straddle_strangle_margin(100.0, 110.0, 90.0, 0.5, 0.5)
        assert result > 0


# ---------------------------------------------------------------------------
# covered_call_margin
# ---------------------------------------------------------------------------


class TestCoveredCallMargin:
    """Tests for covered_call_margin(underlying_price)."""

    def test_standard(self):
        """Stock at $100 -> $10,000 per 100 shares."""
        result = covered_call_margin(100.0)
        assert result == 10_000.0

    def test_zero_underlying(self):
        """Zero price -> $0."""
        result = covered_call_margin(0.0)
        assert result == 0.0

    def test_always_non_negative(self):
        """Result must be non-negative."""
        for p in [0.0, 1.0, 100.0, 500.0]:
            assert covered_call_margin(p) >= 0

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="underlying_price must be non-negative"):
            covered_call_margin(-1.0)


# ---------------------------------------------------------------------------
# cash_secured_put_margin
# ---------------------------------------------------------------------------


class TestCashSecuredPutMargin:
    """Tests for cash_secured_put_margin(strike)."""

    def test_standard(self):
        """Strike $100 -> $10,000 per contract."""
        result = cash_secured_put_margin(100.0)
        assert result == 10_000.0

    def test_zero_strike(self):
        """Zero strike -> $0."""
        result = cash_secured_put_margin(0.0)
        assert result == 0.0

    def test_always_non_negative(self):
        """Result must be non-negative."""
        for s in [0.0, 50.0, 100.0]:
            assert cash_secured_put_margin(s) >= 0

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="strike must be non-negative"):
            cash_secured_put_margin(-1.0)


# ---------------------------------------------------------------------------
# covered_strangle_margin
# ---------------------------------------------------------------------------


class TestCoveredStrangleMargin:
    """Tests for covered_strangle_margin(underlying_price, put_strike, put_premium_per_share)."""

    def test_standard(self):
        """Stock cost + naked put margin."""
        # Stock: 100*100 = 10000; naked_put(100, 90, 2) = 1700
        result = covered_strangle_margin(100.0, 90.0, 2.0)
        assert result == 10_000.0 + 1700.0
        assert result == 11_700.0

    def test_atm_put(self):
        """ATM put: higher naked put margin."""
        # Stock: 10000; naked_put(100, 100, 2) = 2700
        result = covered_strangle_margin(100.0, 100.0, 2.0)
        assert result == 12_700.0

    def test_zero_premium(self):
        """Zero put premium."""
        result = covered_strangle_margin(100.0, 90.0, 0.0)
        assert result == 10_000.0 + 1500.0  # naked_put with 0 premium = 1500
        assert result == 11_500.0

    def test_always_positive(self):
        """Result must always be positive."""
        result = covered_strangle_margin(50.0, 45.0, 1.0)
        assert result > 0


# ---------------------------------------------------------------------------
# collar_margin
# ---------------------------------------------------------------------------


class TestCollarMargin:
    """Tests for collar_margin(underlying_price)."""

    def test_standard(self):
        """Stock at $100 -> $10,000."""
        result = collar_margin(100.0)
        assert result == 10_000.0

    def test_zero_underlying(self):
        """Zero price -> $0."""
        result = collar_margin(0.0)
        assert result == 0.0

    def test_always_non_negative(self):
        """Result must be non-negative."""
        for p in [0.0, 1.0, 100.0]:
            assert collar_margin(p) >= 0

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="underlying_price must be non-negative"):
            collar_margin(-1.0)


# ---------------------------------------------------------------------------
# jade_lizard_margin
# ---------------------------------------------------------------------------


class TestJadeLizardMargin:
    """Tests for jade_lizard_margin(underlying, put_strike, put_premium, call_spread_width, total_credit)."""

    def test_credit_ge_call_spread_naked_put_only(self):
        """When total_credit >= call_spread_width, margin = naked put only."""
        # naked_put(100, 90, 2) = 1700; call_spread = 5*100 = 500; total_credit = 6*100 = 600
        # 600 >= 500 -> return put_naked = 1700
        result = jade_lizard_margin(100.0, 90.0, 2.0, 5.0, 6.0)
        assert result == 1700.0

    def test_credit_lt_call_spread_max_of_both(self):
        """When total_credit < call_spread_width, margin = max(naked_put, call_spread)."""
        # naked_put = 1700; call_spread = 1000; total_credit = 200 (2*100)
        # 200 < 1000 -> max(1700, 1000) = 1700
        result = jade_lizard_margin(100.0, 90.0, 2.0, 10.0, 2.0)
        assert result == 1700.0

    def test_call_spread_greater_than_naked_put(self):
        """When call spread margin > naked put, return call spread."""
        # naked_put(100, 95, 0.5) for OTM put: 0.25*100 - 5 + 0.5 = 20.5; 0.10*95 + 0.5 = 10
        # per_share = 20.5; naked_put = 2050
        # call_spread = 15*100 = 1500; total_credit = 1 (1*100 = 100)
        # 100 < 1500 -> max(2050, 1500) = 2050
        # Try: put with lower margin, call spread higher
        # naked_put(100, 50, 0.5) = 550 (deep OTM); call_spread = 20*100 = 2000; credit = 1
        # max(550, 2000) = 2000
        result = jade_lizard_margin(100.0, 50.0, 0.50, 20.0, 1.0)
        assert result == 2000.0

    def test_exact_credit_equals_spread(self):
        """When credit exactly equals call spread width, naked put only."""
        result = jade_lizard_margin(100.0, 90.0, 2.0, 5.0, 5.0)
        assert result == 1700.0

    def test_always_positive(self):
        """Result must always be positive."""
        result = jade_lizard_margin(100.0, 90.0, 2.0, 5.0, 6.0)
        assert result > 0


# ---------------------------------------------------------------------------
# ratio_backspread_margin
# ---------------------------------------------------------------------------


class TestRatioBackspreadMargin:
    """Tests for ratio_backspread_margin(short_strike, long_strike)."""

    def test_standard(self):
        """$5 spread width -> $500 margin."""
        result = ratio_backspread_margin(100.0, 105.0)
        assert result == 500.0

    def test_long_below_short(self):
        """Long strike below short: abs(long - short) used."""
        result = ratio_backspread_margin(105.0, 100.0)
        assert result == 500.0

    def test_same_strike(self):
        """Same strike -> $0 margin."""
        result = ratio_backspread_margin(100.0, 100.0)
        assert result == 0.0

    def test_wide_spread(self):
        """$20 spread -> $2000 margin."""
        result = ratio_backspread_margin(100.0, 120.0)
        assert result == 2000.0

    def test_always_non_negative(self):
        """Result must be non-negative."""
        for short, long_ in [(100, 105), (105, 100), (100, 100)]:
            assert ratio_backspread_margin(short, long_) >= 0


# ---------------------------------------------------------------------------
# short_stock_margin
# ---------------------------------------------------------------------------


class TestShortStockMargin:
    """Tests for short_stock_margin(underlying_price)."""

    def test_standard(self):
        """50% of market value: $100 stock -> $5000 per 100 shares."""
        result = short_stock_margin(100.0)
        assert result == 100.0 * 100.0 * 0.50
        assert result == 5000.0

    def test_zero_underlying(self):
        """Zero price -> $0."""
        result = short_stock_margin(0.0)
        assert result == 0.0

    def test_always_non_negative(self):
        """Result must be non-negative."""
        for p in [0.0, 1.0, 100.0, 500.0]:
            assert short_stock_margin(p) >= 0

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="underlying_price must be non-negative"):
            short_stock_margin(-1.0)
