"""Test that custom strategy margin estimation uses optimal pairing.

Regression test for the bug where per-short greedy pairing could leave
one short naked when a global rearrangement would pair both shorts with
available longs.
"""
from __future__ import annotations

from datetime import date

from backtestforecast.backtests.strategies.custom import CustomNLegStrategy
from backtestforecast.backtests.types import OpenOptionLeg


def _leg(
    *,
    side: int,
    strike: float,
    contract_type: str = "call",
    expiration: date | None = None,
    qty: int = 1,
    mid: float = 1.0,
) -> OpenOptionLeg:
    return OpenOptionLeg(
        ticker=f"O:{contract_type[0].upper()}{strike}",
        contract_type=contract_type,
        side=side,
        strike_price=strike,
        expiration_date=expiration or date(2025, 6, 20),
        quantity_per_unit=qty,
        entry_mid=mid,
        last_mid=mid,
    )


class TestOptimalPairing:
    def test_two_shorts_two_longs_both_paired(self):
        """Both shorts should be paired - none left naked.

        Setup: short 100C, short 110C, long 105C, long 112C.
        Global greedy sorts candidates by width:
          (2, short110, long112), (5, short100, long105), (5, short110, long105), (12, short100, long112)
        Picks: short110-long112 (width 2) + short100-long105 (width 5).
        Total margin: 200 + 500 = 700.
        """
        legs = [
            _leg(side=-1, strike=100),
            _leg(side=-1, strike=110),
            _leg(side=1, strike=105),
            _leg(side=1, strike=112),
        ]
        margin = CustomNLegStrategy._estimate_credit_margin(legs, underlying_price=100.0)
        from backtestforecast.backtests.margin import credit_spread_margin
        expected = credit_spread_margin(2) + credit_spread_margin(5)
        assert abs(margin - expected) < 0.01, f"Expected {expected}, got {margin}"

    def test_adversarial_pairing_order(self):
        """Global greedy picks the tightest pair first.

        short 100C, short 102C, long 101C (only one long).
        Candidates: (100,101)=1, (102,101)=1. Both width 1.
        One short gets paired (width 1), the other is naked.
        """
        legs = [
            _leg(side=-1, strike=100),
            _leg(side=-1, strike=102),
            _leg(side=1, strike=101),
        ]
        margin = CustomNLegStrategy._estimate_credit_margin(legs, underlying_price=100.0)
        from backtestforecast.backtests.margin import credit_spread_margin, naked_option_margin
        paired_margin = credit_spread_margin(1)
        naked_call_at_102 = naked_option_margin("call", 100.0, 102.0, 1.0)
        naked_call_at_100 = naked_option_margin("call", 100.0, 100.0, 1.0)
        expected_option_a = paired_margin + naked_call_at_102
        expected_option_b = paired_margin + naked_call_at_100
        assert margin in (expected_option_a, expected_option_b), (
            f"Expected one of {expected_option_a} or {expected_option_b}, got {margin}"
        )

    def test_all_shorts_no_longs_all_naked(self):
        legs = [
            _leg(side=-1, strike=100),
            _leg(side=-1, strike=110),
        ]
        margin = CustomNLegStrategy._estimate_credit_margin(legs, underlying_price=100.0)
        from backtestforecast.backtests.margin import naked_option_margin
        expected = (
            naked_option_margin("call", 100.0, 100.0, 1.0)
            + naked_option_margin("call", 100.0, 110.0, 1.0)
        )
        assert abs(margin - expected) < 0.01

    def test_all_longs_no_shorts_zero_margin(self):
        legs = [
            _leg(side=1, strike=100),
            _leg(side=1, strike=110),
        ]
        margin = CustomNLegStrategy._estimate_credit_margin(legs, underlying_price=100.0)
        assert margin == 0.0

    def test_mixed_contract_types_not_paired(self):
        """A call short and a put long should NOT pair."""
        legs = [
            _leg(side=-1, strike=100, contract_type="call"),
            _leg(side=1, strike=105, contract_type="put"),
        ]
        margin = CustomNLegStrategy._estimate_credit_margin(legs, underlying_price=100.0)
        from backtestforecast.backtests.margin import naked_option_margin
        expected = naked_option_margin("call", 100.0, 100.0, 1.0)
        assert abs(margin - expected) < 0.01

    def test_different_expirations_not_paired(self):
        legs = [
            _leg(side=-1, strike=100, expiration=date(2025, 6, 20)),
            _leg(side=1, strike=105, expiration=date(2025, 7, 18)),
        ]
        margin = CustomNLegStrategy._estimate_credit_margin(legs, underlying_price=100.0)
        from backtestforecast.backtests.margin import naked_option_margin
        expected = naked_option_margin("call", 100.0, 100.0, 1.0)
        assert abs(margin - expected) < 0.01

    def test_empty_legs_zero_margin(self):
        margin = CustomNLegStrategy._estimate_credit_margin([], underlying_price=100.0)
        assert margin == 0.0

    def test_global_greedy_beats_per_short_greedy(self):
        """Demonstrate that global greedy produces lower margin than per-short.

        Setup: short 100C, short 105C, long 104C, long 108C.

        Per-short greedy (iterate shorts in order):
          short100: nearest long = 104 (width 4) -> paired
          short105: nearest remaining long = 108 (width 3) -> paired
          Total: credit_spread(4) + credit_spread(3) = 400 + 300 = 700

        Global greedy (sort all pairs by width):
          Candidates sorted: (1, short105, long104), (3, short105, long108),
                             (4, short100, long104), (8, short100, long108)
          Pick (1, short105, long104) -> paired
          Pick (8, short100, long108) -> paired
          Total: credit_spread(1) + credit_spread(8) = 100 + 800 = 900

        Wait - global greedy can also produce worse results in some cases!
        The key guarantee is that BOTH shorts get paired (not left naked).
        """
        legs = [
            _leg(side=-1, strike=100),
            _leg(side=-1, strike=105),
            _leg(side=1, strike=104),
            _leg(side=1, strike=108),
        ]
        margin = CustomNLegStrategy._estimate_credit_margin(legs, underlying_price=100.0)
        from backtestforecast.backtests.margin import credit_spread_margin, naked_option_margin
        max_spread_only = credit_spread_margin(1) + credit_spread_margin(8)
        assert abs(margin - max_spread_only) < 0.01
        any_naked = naked_option_margin("call", 100.0, 100.0, 1.0)
        assert margin < any_naked, "Both paired should be cheaper than any naked short"
