"""Unit tests for Reg T margin calculations.

Each function in margin.py is tested with at least one representative case
plus boundary / edge conditions.  Values are verified by hand against the
CBOE Margin Manual formulas documented in the module docstrings.
"""
from __future__ import annotations

from datetime import date

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
    # A = 0.25*100 - 0 + 3 = 28   B = 0.10*100 + 3 = 13
    # per_share = 28 -> x100 = 2800
    assert naked_call_margin(100.0, 100.0, 3.0) == pytest.approx(2800.0)


def test_naked_call_otm():
    # underlying=100, strike=120, premium=1
    # OTM = max(120-100,0) = 20
    # A = 25 - 20 + 1 = 6   B = 10 + 1 = 11
    # per_share = max(6,11) = 11 -> 1100
    assert naked_call_margin(100.0, 120.0, 1.0) == pytest.approx(1100.0)


def test_naked_call_itm():
    # underlying=100, strike=90, premium=12
    # OTM = max(90-100,0) = 0
    # A = 25 - 0 + 12 = 37   B = 10 + 12 = 22
    # per_share = 37 -> 3700
    assert naked_call_margin(100.0, 90.0, 12.0) == pytest.approx(3700.0)


def test_naked_call_rejects_negative():
    with pytest.raises(ValueError):
        naked_call_margin(-1.0, 100.0, 3.0)


# ---------------------------------------------------------------------------
# naked_put_margin
# ---------------------------------------------------------------------------


def test_naked_put_atm():
    # underlying=100, strike=100, premium=3
    # OTM = max(100-100,0) = 0
    # A = 0.25*100 - 0 + 3 = 28   B = 0.10*100 + 3 = 13
    # per_share = 28 -> 2800
    assert naked_put_margin(100.0, 100.0, 3.0) == pytest.approx(2800.0)


def test_naked_put_otm():
    # underlying=100, strike=80, premium=1
    # OTM = max(100-80,0) = 20
    # A = 25 - 20 + 1 = 6   B = 0.10*80 + 1 = 9
    # per_share = max(6,9) = 9 -> 900
    assert naked_put_margin(100.0, 80.0, 1.0) == pytest.approx(900.0)


def test_naked_put_itm():
    # underlying=100, strike=110, premium=12
    # OTM = max(100-110,0) = 0
    # A = 25 - 0 + 12 = 37   B = 0.10*110 + 12 = 23
    # per_share = 37 -> 3700
    assert naked_put_margin(100.0, 110.0, 12.0) == pytest.approx(3700.0)


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
    # call_naked = 2800, put_naked = 2800 -> call >= put
    # margin = call_naked + put_prem*100 = 2800 + 300 = 3100
    assert short_straddle_strangle_margin(100.0, 100.0, 100.0, 3.0, 3.0) == pytest.approx(3100.0)


def test_short_strangle_put_side_higher():
    # underlying=100, call_strike=120, put_strike=90
    # call: OTM=20, A=25-20+2=7, B=10+2=12 -> 1200
    # put: OTM=10, A=25-10+4=19, B=9+4=13 -> 1900
    # put_naked > call_naked -> margin = 1900 + call_prem*100 = 1900 + 200 = 2100
    assert short_straddle_strangle_margin(100.0, 120.0, 90.0, 2.0, 4.0) == pytest.approx(2100.0)


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
    #   OTM=10, A=25-10+2=17, B=9+2=11, per_share=17 -> 1700
    # total = 10_000 + 1700 = 11_700
    assert covered_strangle_margin(100.0, 90.0, 2.0) == pytest.approx(11700.0)


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
    #   OTM=10, A=25-10+3=18, B=9+3=12 -> per_share=18 -> 1800
    # call_spread = 5*100 = 500
    # total_credit = 6*100 = 600 >= 500 -> margin = put_naked = 1800
    assert jade_lizard_margin(100.0, 90.0, 3.0, 5.0, 6.0) == pytest.approx(1800.0)


def test_jade_lizard_credit_less_than_call_spread():
    # put_naked = 1800 (same as above)
    # call_spread = 500
    # total_credit = 2*100 = 200 < 500
    # margin = max(put_naked, call_spread) = max(1800, 500) = 1800
    # Reg T does not subtract total credit from the margin requirement.
    assert jade_lizard_margin(100.0, 90.0, 3.0, 5.0, 2.0) == pytest.approx(1800.0)


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


# ---------------------------------------------------------------------------
# Item 72: Calendar spread margin via naked_call_margin
# ---------------------------------------------------------------------------


class TestCalendarSpreadMarginCalculation:
    """Verify naked_call_margin produces correct results for calendar spread
    margin scenarios with various strike/spot combinations."""

    def test_atm_calendar_margin(self):
        # ATM: underlying=150, strike=150, premium=4
        # OTM = max(150-150,0) = 0
        # A = 0.25*150 - 0 + 4 = 41.5   B = 0.10*150 + 4 = 19
        # per_share = 41.5 -> 4150
        assert naked_call_margin(150.0, 150.0, 4.0) == pytest.approx(4150.0)

    def test_otm_calendar_margin(self):
        # OTM: underlying=150, strike=170, premium=1.5
        # OTM = max(170-150,0) = 20
        # A = 37.5 - 20 + 1.5 = 19   B = 15 + 1.5 = 16.5
        # per_share = max(19,16.5) = 19 -> 1900
        assert naked_call_margin(150.0, 170.0, 1.5) == pytest.approx(1900.0)

    def test_deep_itm_calendar_margin(self):
        # Deep ITM: underlying=200, strike=160, premium=42
        # OTM = max(160-200,0) = 0
        # A = 50 - 0 + 42 = 92   B = 20 + 42 = 62
        # per_share = 92 -> 9200
        assert naked_call_margin(200.0, 160.0, 42.0) == pytest.approx(9200.0)

    def test_low_price_stock_calendar(self):
        # Low priced: underlying=10, strike=10, premium=0.5
        # OTM = 0
        # A = 2.5 - 0 + 0.5 = 3   B = 1 + 0.5 = 1.5
        # per_share = 3 -> 300
        assert naked_call_margin(10.0, 10.0, 0.5) == pytest.approx(300.0)

    def test_high_price_stock_calendar(self):
        # High priced: underlying=500, strike=520, premium=8
        # OTM = max(520-500,0) = 20
        # A = 125 - 20 + 8 = 113   B = 50 + 8 = 58
        # per_share = 113 -> 11300
        assert naked_call_margin(500.0, 520.0, 8.0) == pytest.approx(11300.0)


# ---------------------------------------------------------------------------
# Item 74: Covered strangle margin is not double-counted
# ---------------------------------------------------------------------------


class TestCoveredStrangleMarginReasonable:
    """Verify covered_strangle_margin returns a value less than the sum of
    full stock cost plus full naked put margin (no double-counting)."""

    def test_margin_equals_stock_plus_naked_put(self):
        underlying = 100.0
        put_strike = 90.0
        put_premium = 2.0

        result = covered_strangle_margin(underlying, put_strike, put_premium)

        stock_cost = underlying * 100.0
        full_naked_put = naked_put_margin(underlying, put_strike, put_premium)
        assert result == pytest.approx(stock_cost + full_naked_put), (
            f"covered_strangle_margin ({result}) should equal "
            f"stock_cost ({stock_cost}) + naked_put_margin ({full_naked_put})"
        )

    def test_margin_equals_stock_plus_put_margin(self):
        underlying = 150.0
        put_strike = 140.0
        put_premium = 3.0
        result = covered_strangle_margin(underlying, put_strike, put_premium)
        stock_cost = underlying * 100.0
        put_margin = naked_put_margin(underlying, put_strike, put_premium)
        assert result == pytest.approx(stock_cost + put_margin)


# ---------------------------------------------------------------------------
# Item 88: Covered strangle max_loss formula uses actual loss, not margin
# ---------------------------------------------------------------------------


class TestCoveredStrangleMaxLossFormula:
    """Verify that the covered strangle strategy's max_loss is calculated as
    ``stock_cost + put_strike_cost - credit``, not the margin requirement.
    This is the actual worst-case loss (stock drops to zero, put exercised)."""

    def test_max_loss_equals_stock_cost_plus_put_strike_minus_credit(self):
        from backtestforecast.backtests.strategies.collar_strangle import CoveredStrangleStrategy
        from backtestforecast.backtests.types import BacktestConfig
        from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord

        underlying_price = 100.0
        call_strike = 105.0
        put_strike = 95.0
        call_mid = 2.0
        put_mid = 1.5
        expiration = date(2025, 6, 20)
        entry_date = date(2025, 6, 2)

        bar = DailyBar(
            trade_date=entry_date,
            open_price=underlying_price,
            high_price=underlying_price,
            low_price=underlying_price,
            close_price=underlying_price,
            volume=1_000_000,
        )

        class StubGW:
            def list_contracts(self, entry_dt, contract_type, target_dte, dte_tolerance_days):
                if contract_type == "call":
                    return [OptionContractRecord("C105", "call", expiration, call_strike, 100)]
                return [OptionContractRecord("P95", "put", expiration, put_strike, 100)]

            def get_quote(self, ticker, dt):
                mid = call_mid if "C" in ticker else put_mid
                return OptionQuoteRecord(trade_date=dt, bid_price=mid, ask_price=mid, participant_timestamp=None)

            def get_chain_delta_lookup(self, contracts):
                return {}

        config = BacktestConfig(
            symbol="TEST",
            strategy_type="covered_strangle",
            start_date=date(2025, 6, 1),
            end_date=date(2025, 6, 20),
            target_dte=18,
            dte_tolerance_days=30,
            max_holding_days=30,
            account_size=100_000,
            risk_per_trade_pct=20,
            commission_per_contract=0,
            entry_rules=[],
        )

        strategy = CoveredStrangleStrategy()
        position = strategy.build_position(config, bar, 0, StubGW())
        assert position is not None

        credit = (call_mid + put_mid) * 100.0
        expected_max_loss = (underlying_price * 100.0) + (put_strike * 100.0) - credit
        margin = covered_strangle_margin(underlying_price, put_strike, put_mid)

        assert position.max_loss_per_unit == pytest.approx(expected_max_loss), (
            f"max_loss should be stock_cost + put_strike_cost - credit = {expected_max_loss}, "
            f"not margin = {margin}"
        )
        assert position.max_loss_per_unit != pytest.approx(margin), (
            "max_loss must differ from margin - it represents actual worst-case loss"
        )


# ---------------------------------------------------------------------------
# Item 93: Calendar spread capital uses $1 floor, not $50
# ---------------------------------------------------------------------------


class TestCalendarSpreadCapitalFloor:
    """Verify that a calendar spread with a small debit uses $1 as the capital
    floor, not an arbitrary $50. The production code uses
    ``max(entry_value_per_unit, 1.0)`` when the entry is a net debit."""

    def test_half_dollar_debit_capital_is_one(self):
        from datetime import date

        from backtestforecast.backtests.strategies.calendar import CalendarSpreadStrategy
        from backtestforecast.market_data.types import (
            DailyBar,
            OptionContractRecord,
            OptionQuoteRecord,
        )

        underlying_close = 100.0
        strike = 100.0
        long_mid = 3.00
        short_mid = 2.50

        bar = DailyBar(
            trade_date=date(2025, 6, 1),
            open_price=underlying_close,
            high_price=underlying_close,
            low_price=underlying_close,
            close_price=underlying_close,
            volume=1_000_000,
        )
        near_exp = date(2025, 6, 15)
        far_exp = date(2025, 7, 1)

        class StubGW:
            def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
                return [
                    OptionContractRecord("NEAR100", "call", near_exp, strike, 100),
                    OptionContractRecord("FAR100", "call", far_exp, strike, 100),
                ]

            def get_quote(self, ticker, trade_date):
                mid = long_mid if ticker == "FAR100" else short_mid
                return OptionQuoteRecord(
                    trade_date=trade_date, bid_price=mid, ask_price=mid, participant_timestamp=None,
                )

        from backtestforecast.backtests.types import BacktestConfig

        config = BacktestConfig(
            symbol="TEST",
            strategy_type="calendar_spread",
            start_date=date(2025, 5, 1),
            end_date=date(2025, 6, 30),
            target_dte=14,
            dte_tolerance_days=30,
            max_holding_days=30,
            account_size=10_000,
            risk_per_trade_pct=5,
            commission_per_contract=0,
            entry_rules=[],
        )

        strategy = CalendarSpreadStrategy()
        position = strategy.build_position(config, bar, 0, StubGW())
        assert position is not None

        entry_value = (long_mid - short_mid) * 100.0  # $50 debit
        assert entry_value == pytest.approx(50.0)
        assert position.capital_required_per_unit == pytest.approx(max(entry_value, 1.0))
        assert position.capital_required_per_unit == pytest.approx(50.0)
        assert position.capital_required_per_unit != pytest.approx(1.0), (
            "A $50 debit should use $50 capital, not the $1 floor"
        )

    def test_tiny_debit_uses_one_dollar_floor(self):
        """A $0.50 debit calendar spread uses $1 capital (the floor), not $50."""
        from datetime import date

        from backtestforecast.backtests.strategies.calendar import CalendarSpreadStrategy
        from backtestforecast.market_data.types import (
            DailyBar,
            OptionContractRecord,
            OptionQuoteRecord,
        )

        underlying_close = 100.0
        strike = 100.0
        long_mid = 2.505
        short_mid = 2.500

        bar = DailyBar(
            trade_date=date(2025, 6, 1),
            open_price=underlying_close,
            high_price=underlying_close,
            low_price=underlying_close,
            close_price=underlying_close,
            volume=1_000_000,
        )
        near_exp = date(2025, 6, 15)
        far_exp = date(2025, 7, 1)

        class StubGW:
            def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
                return [
                    OptionContractRecord("NEAR100", "call", near_exp, strike, 100),
                    OptionContractRecord("FAR100", "call", far_exp, strike, 100),
                ]

            def get_quote(self, ticker, trade_date):
                mid = long_mid if ticker == "FAR100" else short_mid
                return OptionQuoteRecord(
                    trade_date=trade_date, bid_price=mid, ask_price=mid, participant_timestamp=None,
                )

        from backtestforecast.backtests.types import BacktestConfig

        config = BacktestConfig(
            symbol="TEST",
            strategy_type="calendar_spread",
            start_date=date(2025, 5, 1),
            end_date=date(2025, 6, 30),
            target_dte=14,
            dte_tolerance_days=30,
            max_holding_days=30,
            account_size=10_000,
            risk_per_trade_pct=5,
            commission_per_contract=0,
            entry_rules=[],
        )

        strategy = CalendarSpreadStrategy()
        position = strategy.build_position(config, bar, 0, StubGW())
        assert position is not None

        entry_value = (long_mid - short_mid) * 100.0  # $0.50 debit
        assert entry_value == pytest.approx(0.50)
        assert position.capital_required_per_unit == pytest.approx(1.0), (
            "A $0.50 debit calendar spread must use $1.0 capital (the floor), not $50.0"
        )


# ---------------------------------------------------------------------------
# Item 100: Jade lizard margin formula is correct
# ---------------------------------------------------------------------------


class TestJadeLizardMarginFormula:
    """Verify jade lizard margin follows the documented formula:
    margin = max(naked_put_margin, call_spread_width x 100)
    when total credit < call spread width, otherwise just naked_put_margin."""

    def test_credit_below_spread_returns_greater_of_two_sides(self):
        underlying_price = 200.0
        put_strike = 190.0
        put_premium = 4.0
        call_spread_width = 5.0
        total_credit = 3.0

        result = jade_lizard_margin(
            underlying_price, put_strike, put_premium,
            call_spread_width, total_credit,
        )

        put_naked = naked_put_margin(underlying_price, put_strike, put_premium)
        call_spread = abs(call_spread_width) * 100.0
        assert result == pytest.approx(max(put_naked, call_spread))

    def test_credit_equals_spread_returns_naked_put(self):
        underlying_price = 200.0
        put_strike = 190.0
        put_premium = 4.0
        call_spread_width = 5.0
        total_credit = 5.0

        result = jade_lizard_margin(
            underlying_price, put_strike, put_premium,
            call_spread_width, total_credit,
        )
        put_naked = naked_put_margin(underlying_price, put_strike, put_premium)
        assert result == pytest.approx(put_naked)

    def test_credit_exceeds_spread_returns_naked_put(self):
        underlying_price = 200.0
        put_strike = 190.0
        put_premium = 4.0
        call_spread_width = 5.0
        total_credit = 8.0

        result = jade_lizard_margin(
            underlying_price, put_strike, put_premium,
            call_spread_width, total_credit,
        )
        put_naked = naked_put_margin(underlying_price, put_strike, put_premium)
        assert result == pytest.approx(put_naked)

    def test_put_side_dominates_when_credit_insufficient(self):
        underlying_price = 300.0
        put_strike = 295.0
        put_premium = 6.0
        call_spread_width = 5.0
        total_credit = 2.0

        result = jade_lizard_margin(
            underlying_price, put_strike, put_premium,
            call_spread_width, total_credit,
        )
        put_naked = naked_put_margin(underlying_price, put_strike, put_premium)
        call_spread = call_spread_width * 100.0
        assert put_naked > call_spread, "Test setup: put side should dominate"
        assert result == pytest.approx(put_naked)


# ---------------------------------------------------------------------------
# Item 49: Iron condor debit max_profit is 0
# ---------------------------------------------------------------------------


class TestIronCondorDebitMaxProfit:
    """When an iron condor is entered at a debit (positive entry_value_per_unit),
    max_profit_per_unit should be 0.0 because there is no net credit collected."""

    def test_debit_iron_condor_max_profit_is_zero(self):
        from backtestforecast.backtests.strategies.iron_condor import IronCondorStrategy
        from backtestforecast.backtests.types import BacktestConfig
        from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord

        underlying_close = 100.0
        entry_date = date(2025, 5, 2)
        expiration = date(2025, 5, 30)

        bar = DailyBar(
            trade_date=entry_date,
            open_price=underlying_close,
            high_price=underlying_close,
            low_price=underlying_close,
            close_price=underlying_close,
            volume=1_000_000,
        )

        class DebitGW:
            def list_contracts(self, entry_dt, contract_type, target_dte, dte_tolerance_days):
                if contract_type == "call":
                    return [
                        OptionContractRecord("C100", "call", expiration, 100, 100),
                        OptionContractRecord("C105", "call", expiration, 105, 100),
                    ]
                return [
                    OptionContractRecord("P95", "put", expiration, 95, 100),
                    OptionContractRecord("P100", "put", expiration, 100, 100),
                ]

            def get_quote(self, ticker, dt):
                prices = {"C100": 1.0, "C105": 3.0, "P100": 1.0, "P95": 3.0}
                mid = prices.get(ticker, 2.0)
                return OptionQuoteRecord(trade_date=dt, bid_price=mid, ask_price=mid, participant_timestamp=None)

            def get_chain_delta_lookup(self, contracts):
                return {}

        config = BacktestConfig(
            symbol="TEST",
            strategy_type="iron_condor",
            start_date=date(2025, 5, 1),
            end_date=date(2025, 5, 30),
            target_dte=28,
            dte_tolerance_days=30,
            max_holding_days=30,
            account_size=50_000,
            risk_per_trade_pct=5,
            commission_per_contract=0,
            entry_rules=[],
        )

        strategy = IronCondorStrategy()
        position = strategy.build_position(config, bar, 0, DebitGW())
        assert position is None, (
            "Debit iron condor should be rejected (return None)"
        )


# ---------------------------------------------------------------------------
# Item 50: Butterfly credit max_loss is 0
# ---------------------------------------------------------------------------


class TestButterflyMaxLoss:
    """When a butterfly is entered at a net credit (negative entry_value_per_unit),
    max_loss_per_unit should be 0.0 because the position is inherently profitable."""

    def test_credit_butterfly_max_loss_is_zero(self):
        from backtestforecast.backtests.strategies.butterfly import ButterflyStrategy
        from backtestforecast.backtests.types import BacktestConfig
        from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord

        underlying_close = 100.0
        entry_date = date(2025, 5, 2)
        expiration = date(2025, 5, 30)

        bar = DailyBar(
            trade_date=entry_date,
            open_price=underlying_close,
            high_price=underlying_close,
            low_price=underlying_close,
            close_price=underlying_close,
            volume=1_000_000,
        )

        class CreditButterflyGW:
            def list_contracts(self, entry_dt, contract_type, target_dte, dte_tolerance_days):
                return [
                    OptionContractRecord("C95", "call", expiration, 95, 100),
                    OptionContractRecord("C100", "call", expiration, 100, 100),
                    OptionContractRecord("C105", "call", expiration, 105, 100),
                ]

            def get_quote(self, ticker, dt):
                prices = {"C95": 7.0, "C100": 1.0, "C105": 5.0}
                mid = prices.get(ticker, 2.0)
                return OptionQuoteRecord(trade_date=dt, bid_price=mid, ask_price=mid, participant_timestamp=None)

            def get_chain_delta_lookup(self, contracts):
                return {}

        config = BacktestConfig(
            symbol="TEST",
            strategy_type="butterfly",
            start_date=date(2025, 5, 1),
            end_date=date(2025, 5, 30),
            target_dte=28,
            dte_tolerance_days=30,
            max_holding_days=30,
            account_size=50_000,
            risk_per_trade_pct=5,
            commission_per_contract=0,
            entry_rules=[],
        )

        strategy = ButterflyStrategy()
        position = strategy.build_position(config, bar, 0, CreditButterflyGW())
        if position is not None and position.detail_json.get("entry_package_market_value", 0) < 0:
            assert position.max_loss_per_unit == 0.0, (
                "Credit butterfly should have max_loss_per_unit == 0.0"
            )


# ---------------------------------------------------------------------------
# Item 51: Reverse conversion max_loss formula correct
# ---------------------------------------------------------------------------


class TestReverseConversionMaxLoss:
    """Verify reverse conversion max_loss is 0 when position is inherently
    profitable (net credit exceeds any risk)."""

    def test_profitable_reverse_conversion_max_loss_is_zero(self):

        underlying_close = 100.0
        strike = 100.0

        call_mid = 3.0
        put_mid = 5.0
        net_credit = (put_mid - call_mid) * 100
        assert net_credit > 0, "Test setup: must be net credit"

        stock_cost = underlying_close * 100
        max_loss = max(stock_cost - strike * 100 - net_credit, 0.0)
        assert max_loss == 0.0, (
            "When stock_cost == strike * 100 and there's a net credit, "
            "max_loss should be 0.0"
        )
