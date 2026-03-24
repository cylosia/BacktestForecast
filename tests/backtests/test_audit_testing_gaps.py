"""Tests covering gaps identified in the production audit.

TG1:  resolve_strike with DELTA_TARGET mode
TG2:  resolve_wing_strike with DOLLAR_WIDTH and PCT_WIDTH
TG3:  custom strategy all-short legs (fully naked) -> max_loss returns None
TG4:  custom strategy all-long legs -> max_loss returns debit
TG5:  PMCC _deep_itm_call_strike depth selection
TG6:  Extreme market conditions (penny stock, high-priced stock)
TG7:  _approx_bsm_delta realized_vol fallback
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from backtestforecast.backtests.strategies.common import (
    SpreadWidthConfig,
    SpreadWidthMode,
    StrikeSelection,
    StrikeSelectionMode,
    _approx_bsm_delta,
    resolve_strike,
    resolve_wing_strike,
)
from backtestforecast.backtests.strategies.custom import CustomNLegStrategy
from backtestforecast.backtests.strategies.diagonal import _deep_itm_call_strike
from backtestforecast.backtests.types import OpenOptionLeg
from backtestforecast.errors import DataUnavailableError

# ---------------------------------------------------------------------------
# TG1 / TG2: resolve_strike - DELTA_TARGET mode
# ---------------------------------------------------------------------------


class TestResolveStrikeDeltaTarget:
    STRIKES = [90.0, 95.0, 100.0, 105.0, 110.0, 115.0, 120.0]

    def test_call_delta_30_selects_otm(self):
        """A 30-delta call target should select a strike above spot."""
        sel = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))
        result = resolve_strike(self.STRIKES, 100.0, "call", sel, dte_days=30)
        assert result > 100.0, f"30-delta call should be OTM, got strike {result}"

    def test_put_delta_30_selects_otm(self):
        """A 30-delta put target should select a strike below spot."""
        sel = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))
        result = resolve_strike(self.STRIKES, 100.0, "put", sel, dte_days=30)
        assert result < 100.0, f"30-delta put should be OTM, got strike {result}"

    def test_delta_50_selects_near_atm(self):
        """A 50-delta target should select ATM or very near ATM."""
        sel = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("50"))
        result = resolve_strike(self.STRIKES, 100.0, "call", sel, dte_days=30)
        assert abs(result - 100.0) <= 5.0, f"50-delta should be near ATM, got {result}"

    def test_delta_target_with_realized_vol(self):
        """realized_vol should be used as fallback when IV is unavailable."""
        sel = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))
        resolve_strike(self.STRIKES, 100.0, "call", sel, dte_days=30)
        result_high_vol = resolve_strike(
            self.STRIKES, 100.0, "call", sel, dte_days=30, realized_vol=0.80,
        )
        assert isinstance(result_high_vol, float)
        assert result_high_vol in self.STRIKES

    def test_delta_target_with_delta_lookup(self):
        """Pre-built delta lookup should take priority."""
        sel = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))
        lookup = {105.0: 0.35, 110.0: 0.25, 115.0: 0.15}
        result = resolve_strike(
            self.STRIKES, 100.0, "call", sel, dte_days=30,
            delta_lookup=lookup,
        )
        assert result == 105.0, f"Closest to 0.30 delta is 105 (0.35), got {result}"

    def test_delta_target_empty_strikes_raises(self):
        sel = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))
        with pytest.raises(DataUnavailableError, match="No strikes"):
            resolve_strike([], 100.0, "call", sel, dte_days=30)


# ---------------------------------------------------------------------------
# TG3: resolve_wing_strike - DOLLAR_WIDTH and PCT_WIDTH
# ---------------------------------------------------------------------------


class TestResolveWingStrike:
    STRIKES = [85.0, 90.0, 95.0, 100.0, 105.0, 110.0, 115.0]

    def test_dollar_width_call_wing(self):
        """DOLLAR_WIDTH of $10 above 100 should select ~110."""
        cfg = SpreadWidthConfig(mode=SpreadWidthMode.DOLLAR_WIDTH, value=Decimal("10"))
        result = resolve_wing_strike(self.STRIKES, 100.0, 1, 100.0, cfg)
        assert result == 110.0

    def test_dollar_width_put_wing(self):
        """DOLLAR_WIDTH of $10 below 100 should select ~90."""
        cfg = SpreadWidthConfig(mode=SpreadWidthMode.DOLLAR_WIDTH, value=Decimal("10"))
        result = resolve_wing_strike(self.STRIKES, 100.0, -1, 100.0, cfg)
        assert result == 90.0

    def test_pct_width_call_wing(self):
        """PCT_WIDTH of 5% on $100 = $5 above 100 -> 105."""
        cfg = SpreadWidthConfig(mode=SpreadWidthMode.PCT_WIDTH, value=Decimal("5"))
        result = resolve_wing_strike(self.STRIKES, 100.0, 1, 100.0, cfg)
        assert result == 105.0

    def test_pct_width_put_wing(self):
        """PCT_WIDTH of 5% on $100 = $5 below 100 -> 95."""
        cfg = SpreadWidthConfig(mode=SpreadWidthMode.PCT_WIDTH, value=Decimal("5"))
        result = resolve_wing_strike(self.STRIKES, 100.0, -1, 100.0, cfg)
        assert result == 95.0

    def test_wrong_side_correction(self):
        """If the nearest strike lands on the wrong side, fallback to offset."""
        cfg = SpreadWidthConfig(mode=SpreadWidthMode.DOLLAR_WIDTH, value=Decimal("2"))
        result = resolve_wing_strike(self.STRIKES, 100.0, 1, 100.0, cfg)
        assert result is not None
        assert result > 100.0, f"Call wing must be above short strike, got {result}"

    def test_no_valid_strike_returns_none(self):
        """Single strike can't produce a wing."""
        result = resolve_wing_strike([100.0], 100.0, 1, 100.0, None)
        assert result is None

    def test_strike_steps_mode(self):
        """STRIKE_STEPS of 2 should skip one strike."""
        cfg = SpreadWidthConfig(mode=SpreadWidthMode.STRIKE_STEPS, value=Decimal("2"))
        result = resolve_wing_strike(self.STRIKES, 100.0, 1, 100.0, cfg)
        assert result == 110.0


# ---------------------------------------------------------------------------
# TG6: Custom strategy max_loss edge cases
# ---------------------------------------------------------------------------


def _make_leg(
    contract_type: str = "call",
    side: int = -1,
    strike: float = 100.0,
    qty: int = 1,
    exp: date | None = None,
) -> OpenOptionLeg:
    return OpenOptionLeg(
        ticker=f"O:{contract_type[0].upper()}{strike}",
        contract_type=contract_type,
        side=side,
        strike_price=strike,
        expiration_date=exp or date(2025, 6, 20),
        quantity_per_unit=qty,
        entry_mid=2.0,
        last_mid=2.0,
    )


class TestCustomStrategyMaxLoss:
    def test_all_short_returns_none(self):
        """Fully naked position -> unlimited risk -> None."""
        legs = [_make_leg(side=-1, strike=100.0), _make_leg(side=-1, strike=110.0)]
        result = CustomNLegStrategy._estimate_max_loss(legs, net_cost=-400.0, underlying_price=105.0)
        assert result is None

    def test_all_long_returns_debit(self):
        """All-long -> max loss = net debit."""
        legs = [_make_leg(side=1, strike=100.0), _make_leg(side=1, strike=110.0)]
        result = CustomNLegStrategy._estimate_max_loss(legs, net_cost=500.0, underlying_price=105.0)
        assert result == 500.0

    def test_hedged_spread_returns_width(self):
        """Bull put spread: short 100, long 95 -> width = $5 x 100 = $500."""
        legs = [
            _make_leg(contract_type="put", side=-1, strike=100.0),
            _make_leg(contract_type="put", side=1, strike=95.0),
        ]
        result = CustomNLegStrategy._estimate_max_loss(legs, net_cost=-200.0, underlying_price=105.0)
        assert result == 500.0

    def test_iron_condor_max_loss(self):
        """Iron condor: max loss = widest side x 100."""
        legs = [
            _make_leg(contract_type="call", side=-1, strike=110.0),
            _make_leg(contract_type="call", side=1, strike=115.0),
            _make_leg(contract_type="put", side=-1, strike=90.0),
            _make_leg(contract_type="put", side=1, strike=85.0),
        ]
        result = CustomNLegStrategy._estimate_max_loss(legs, net_cost=-300.0, underlying_price=100.0)
        assert result == 500.0

    def test_mixed_expiration_leaves_naked(self):
        """Short and long with different expirations can't pair -> naked."""
        legs = [
            _make_leg(contract_type="call", side=-1, strike=100.0, exp=date(2025, 6, 20)),
            _make_leg(contract_type="call", side=1, strike=105.0, exp=date(2025, 7, 18)),
        ]
        result = CustomNLegStrategy._estimate_max_loss(legs, net_cost=-100.0, underlying_price=100.0)
        assert result is None

    def test_mixed_contract_type_leaves_naked(self):
        """Short call + long put can't pair -> naked."""
        legs = [
            _make_leg(contract_type="call", side=-1, strike=100.0),
            _make_leg(contract_type="put", side=1, strike=95.0),
        ]
        result = CustomNLegStrategy._estimate_max_loss(legs, net_cost=-100.0, underlying_price=100.0)
        assert result is None


# ---------------------------------------------------------------------------
# TG7: PMCC _deep_itm_call_strike
# ---------------------------------------------------------------------------


class TestDeepItmCallStrike:
    def test_targets_10pct_below_spot(self):
        """With wide strike spacing, should select near 90% of spot."""
        strikes = [80.0, 85.0, 90.0, 95.0, 100.0, 105.0, 110.0]
        result = _deep_itm_call_strike(strikes, 100.0)
        assert result <= 95.0, f"Should be at least 5% below spot, got {result}"

    def test_narrow_spacing_goes_deep(self):
        """With $1 spacing, should still go deep ITM."""
        strikes = [float(s) for s in range(90, 111)]
        result = _deep_itm_call_strike(strikes, 100.0)
        assert result <= 95.0, f"Should go deep even with narrow spacing, got {result}"

    def test_no_itm_strikes_raises(self):
        """All strikes at or above underlying -> DataUnavailableError."""
        strikes = [100.0, 105.0, 110.0]
        with pytest.raises(DataUnavailableError):
            _deep_itm_call_strike(strikes, 100.0)

    def test_only_one_strike_below(self):
        """Single ITM strike -> returns it."""
        strikes = [98.0, 100.0, 105.0]
        result = _deep_itm_call_strike(strikes, 100.0)
        assert result == 98.0

    def test_two_strikes_below_no_deep_candidates(self):
        """Two strikes below but neither 5% deep -> returns second-to-last."""
        strikes = [97.0, 99.0, 100.0, 105.0]
        result = _deep_itm_call_strike(strikes, 100.0)
        assert result == 97.0


# ---------------------------------------------------------------------------
# TG5: Extreme market conditions
# ---------------------------------------------------------------------------


class TestExtremePriceConditions:
    def test_penny_stock_delta_target(self):
        """Delta targeting should work for a $0.50 stock."""
        strikes = [0.25, 0.50, 0.75, 1.00, 1.25, 1.50]
        sel = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))
        result = resolve_strike(strikes, 0.50, "put", sel, dte_days=30)
        assert result in strikes

    def test_high_priced_stock_pct_from_spot(self):
        """PCT_FROM_SPOT should work for a $5000 stock."""
        strikes = [4900.0, 4950.0, 5000.0, 5050.0, 5100.0]
        sel = StrikeSelection(mode=StrikeSelectionMode.PCT_FROM_SPOT, value=Decimal("1"))
        result = resolve_strike(strikes, 5000.0, "call", sel, dte_days=30)
        assert result == 5050.0

    def test_wing_strike_penny_stock(self):
        """Wing resolution should work for cheap stocks using step mode."""
        strikes = [0.25, 0.50, 0.75, 1.00]
        cfg = SpreadWidthConfig(mode=SpreadWidthMode.STRIKE_STEPS, value=Decimal("1"))
        result = resolve_wing_strike(strikes, 0.50, 1, 0.50, cfg)
        assert result == 0.75


# ---------------------------------------------------------------------------
# TG7: BSM delta with realized vol fallback
# ---------------------------------------------------------------------------


class TestBsmDeltaRealizedVolFallback:
    def test_default_vol_30pct(self):
        """Default fallback vol is ~30%."""
        delta = _approx_bsm_delta(100.0, 105.0, 30, "call")
        assert 0.0 < abs(delta) < 1.0

    def test_high_vol_widens_delta_range(self):
        """With 200% vol (meme stock), OTM strikes have higher delta."""
        delta_low_vol = _approx_bsm_delta(100.0, 130.0, 30, "call", vol=0.20)
        delta_high_vol = _approx_bsm_delta(100.0, 130.0, 30, "call", vol=2.00)
        assert abs(delta_high_vol) > abs(delta_low_vol), (
            f"Higher vol should give higher delta for OTM: low={delta_low_vol}, high={delta_high_vol}"
        )

    def test_zero_dte_call_itm(self):
        """At expiration, ITM call delta -> 1.0."""
        delta = _approx_bsm_delta(100.0, 95.0, 0, "call")
        assert delta == 1.0

    def test_zero_dte_put_otm(self):
        """At expiration, OTM put delta -> 0.0."""
        delta = _approx_bsm_delta(100.0, 95.0, 0, "put")
        assert delta == 0.0

    def test_put_delta_is_negative(self):
        """Put delta should be negative."""
        delta = _approx_bsm_delta(100.0, 95.0, 30, "put")
        assert delta < 0
