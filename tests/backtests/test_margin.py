"""Unit tests for Reg T margin calculations.

Each function in margin.py is tested with at least one representative case
plus boundary / edge conditions.  Values are verified by hand against the
CBOE Margin Manual formulas documented in the module docstrings.
"""
from __future__ import annotations

import pytest

from backtestforecast.backtests.margin import (
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
    short_stock_margin,
    short_straddle_strangle_margin,
)


# ---------------------------------------------------------------------------
# _require_non_negative
# ---------------------------------------------------------------------------


def test_require_non_negative_passes_for_zero_and_positive():
    _require_non_negative(a=0.0, b=1.5)


def test_require_non_negative_raises_for_negative():
    with pytest.raises(ValueError, match="x must be non-negative"):
        _require_non_negative(x=-0.01)


# ---------------------------------------------------------------------------
# naked_call_margin
# ---------------------------------------------------------------------------


def test_naked_call_atm():
    # underlying=100, strike=100, premium=3
    # OTM_amount = max(100-100,0) = 0
    # A = 0.20*100 - 0 + 3 = 23   B = 0.10*100 + 3 = 13
    # per_share = 23 → ×100 = 2300
    assert naked_call_margin(100.0, 100.0, 3.0) == pytest.approx(2300.0)


def test_naked_call_otm():
    # underlying=100, strike=120, premium=1
    # OTM = max(120-100,0) = 20
    # A = 20 - 20 + 1 = 1   B = 10 + 1 = 11
    # per_share = max(1,11) = 11 → 1100
    assert naked_call_margin(100.0, 120.0, 1.0) == pytest.approx(1100.0)


def test_naked_call_itm():
    # underlying=100, strike=90, premium=12
    # OTM = max(90-100,0) = 0
    # A = 20 - 0 + 12 = 32   B = 10 + 12 = 22
    # per_share = 32 → 3200
    assert naked_call_margin(100.0, 90.0, 12.0) == pytest.approx(3200.0)


def test_naked_call_rejects_negative():
    with pytest.raises(ValueError):
        naked_call_margin(-1.0, 100.0, 3.0)


# ---------------------------------------------------------------------------
# naked_put_margin
# ---------------------------------------------------------------------------


def test_naked_put_atm():
    # underlying=100, strike=100, premium=3
    # OTM = max(100-100,0) = 0
    # A = 0.20*100 - 0 + 3 = 23   B = 0.10*100 + 3 = 13
    # per_share = 23 → 2300
    assert naked_put_margin(100.0, 100.0, 3.0) == pytest.approx(2300.0)


def test_naked_put_otm():
    # underlying=100, strike=80, premium=1
    # OTM = max(100-80,0) = 20
    # A = 20 - 20 + 1 = 1   B = 0.10*80 + 1 = 9
    # per_share = max(1,9) = 9 → 900
    assert naked_put_margin(100.0, 80.0, 1.0) == pytest.approx(900.0)


def test_naked_put_itm():
    # underlying=100, strike=110, premium=12
    # OTM = max(100-110,0) = 0
    # A = 20 - 0 + 12 = 32   B = 0.10*110 + 12 = 23
    # per_share = 32 → 3200
    assert naked_put_margin(100.0, 110.0, 12.0) == pytest.approx(3200.0)


def test_naked_put_rejects_negative():
    with pytest.raises(ValueError):
        naked_put_margin(100.0, -5.0, 3.0)


# ---------------------------------------------------------------------------
# naked_option_margin (dispatcher)
# ---------------------------------------------------------------------------


def test_naked_option_margin_dispatches_call():
    assert naked_option_margin("call", 100.0, 100.0, 3.0) == naked_call_margin(100.0, 100.0, 3.0)


def test_naked_option_margin_dispatches_put():
    assert naked_option_margin("put", 100.0, 100.0, 3.0) == naked_put_margin(100.0, 100.0, 3.0)


# ---------------------------------------------------------------------------
# short_straddle_strangle_margin
# ---------------------------------------------------------------------------


def test_short_straddle_margin():
    # underlying=100, call_strike=100, put_strike=100, call_prem=3, put_prem=3
    # call_naked = 2300, put_naked = 2300 → call >= put
    # margin = call_naked + put_prem*100 = 2300 + 300 = 2600
    assert short_straddle_strangle_margin(100.0, 100.0, 100.0, 3.0, 3.0) == pytest.approx(2600.0)


def test_short_strangle_put_side_higher():
    # underlying=100, call_strike=120, put_strike=90
    # call: OTM=20, A=20-20+2=2, B=10+2=12 → 1200
    # put: OTM=10, A=20-10+4=14, B=9+4=13 → 1400
    # put_naked > call_naked → margin = 1400 + call_prem*100 = 1400 + 200 = 1600
    assert short_straddle_strangle_margin(100.0, 120.0, 90.0, 2.0, 4.0) == pytest.approx(1600.0)


# ---------------------------------------------------------------------------
# credit_spread_margin
# ---------------------------------------------------------------------------


def test_credit_spread_margin():
    assert credit_spread_margin(5.0) == pytest.approx(500.0)


def test_credit_spread_negative_width_uses_abs():
    assert credit_spread_margin(-5.0) == pytest.approx(500.0)


# ---------------------------------------------------------------------------
# iron_condor_margin
# ---------------------------------------------------------------------------


def test_iron_condor_equal_widths():
    assert iron_condor_margin(5.0, 5.0) == pytest.approx(500.0)


def test_iron_condor_unequal_widths():
    # Greater width dominates
    assert iron_condor_margin(5.0, 10.0) == pytest.approx(1000.0)


# ---------------------------------------------------------------------------
# covered_call_margin
# ---------------------------------------------------------------------------


def test_covered_call_margin():
    assert covered_call_margin(150.0) == pytest.approx(15000.0)


def test_covered_call_rejects_negative():
    with pytest.raises(ValueError):
        covered_call_margin(-10.0)


# ---------------------------------------------------------------------------
# cash_secured_put_margin
# ---------------------------------------------------------------------------


def test_cash_secured_put_margin():
    assert cash_secured_put_margin(50.0) == pytest.approx(5000.0)


def test_cash_secured_put_rejects_negative():
    with pytest.raises(ValueError):
        cash_secured_put_margin(-1.0)


# ---------------------------------------------------------------------------
# covered_strangle_margin
# ---------------------------------------------------------------------------


def test_covered_strangle_margin():
    # stock_cost = 100*100 = 10_000
    # put_margin: underlying=100, strike=90, premium=2
    #   OTM=10, A=20-10+2=12, B=9+2=11, per_share=12 → 1200
    # total = 10_000 + 1200 = 11_200
    assert covered_strangle_margin(100.0, 90.0, 2.0) == pytest.approx(11200.0)


# ---------------------------------------------------------------------------
# collar_margin
# ---------------------------------------------------------------------------


def test_collar_margin():
    assert collar_margin(200.0) == pytest.approx(20000.0)


def test_collar_rejects_negative():
    with pytest.raises(ValueError):
        collar_margin(-5.0)


# ---------------------------------------------------------------------------
# jade_lizard_margin
# ---------------------------------------------------------------------------


def test_jade_lizard_credit_exceeds_call_spread():
    # put_naked: underlying=100, strike=90, premium=3
    #   OTM=10, A=20-10+3=13, B=9+3=12 → per_share=13 → 1300
    # call_spread = 5*100 = 500
    # total_credit = 6*100 = 600 >= 500 → margin = put_naked = 1300
    assert jade_lizard_margin(100.0, 90.0, 3.0, 5.0, 6.0) == pytest.approx(1300.0)


def test_jade_lizard_credit_less_than_call_spread():
    # put_naked = 1300 (same as above)
    # call_spread = 500
    # total_credit = 2*100 = 200 < 500
    # margin = max(0, max(1300, 500) - 200) = 1100
    assert jade_lizard_margin(100.0, 90.0, 3.0, 5.0, 2.0) == pytest.approx(1100.0)


# ---------------------------------------------------------------------------
# ratio_backspread_margin
# ---------------------------------------------------------------------------


def test_ratio_backspread_margin():
    assert ratio_backspread_margin(100.0, 110.0) == pytest.approx(1000.0)


def test_ratio_backspread_order_independent():
    assert ratio_backspread_margin(110.0, 100.0) == pytest.approx(1000.0)


# ---------------------------------------------------------------------------
# short_stock_margin
# ---------------------------------------------------------------------------


def test_short_stock_margin():
    # 50% of 100*100 = 5000
    assert short_stock_margin(100.0) == pytest.approx(5000.0)


def test_short_stock_rejects_negative():
    with pytest.raises(ValueError):
        short_stock_margin(-1.0)
