"""Reg T margin calculations for options positions.

Reference: CBOE Margin Manual, FINRA Rule 4210.

All functions return margin per contract (i.e., per 100 shares of underlying).
Callers multiply by contract quantity as needed.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Naked options (no stock or spread protection)
# ---------------------------------------------------------------------------


def naked_call_margin(
    underlying_price: float,
    strike: float,
    premium_per_share: float,
) -> float:
    """Reg T margin for a naked (uncovered) short call.

    Greater of:
      A) 20% of underlying − OTM amount + premium
      B) 10% of underlying + premium

    Returns margin per contract (× 100).
    """
    otm_amount = max(strike - underlying_price, 0.0)
    method_a = 0.20 * underlying_price - otm_amount + premium_per_share
    method_b = 0.10 * underlying_price + premium_per_share
    per_share = max(method_a, method_b)
    return max(per_share, 0.0) * 100.0


def naked_put_margin(
    underlying_price: float,
    strike: float,
    premium_per_share: float,
) -> float:
    """Reg T margin for a naked (uncovered) short put.

    Greater of:
      A) 20% of underlying − OTM amount + premium
      B) 10% of strike + premium

    Returns margin per contract (× 100).
    """
    otm_amount = max(underlying_price - strike, 0.0)
    method_a = 0.20 * underlying_price - otm_amount + premium_per_share
    method_b = 0.10 * strike + premium_per_share
    per_share = max(method_a, method_b)
    return max(per_share, 0.0) * 100.0


def naked_option_margin(
    contract_type: str,
    underlying_price: float,
    strike: float,
    premium_per_share: float,
) -> float:
    """Dispatch to the correct naked margin formula by contract type."""
    if contract_type == "call":
        return naked_call_margin(underlying_price, strike, premium_per_share)
    return naked_put_margin(underlying_price, strike, premium_per_share)


# ---------------------------------------------------------------------------
# Short straddle / strangle
# ---------------------------------------------------------------------------


def short_straddle_strangle_margin(
    underlying_price: float,
    call_strike: float,
    put_strike: float,
    call_premium_per_share: float,
    put_premium_per_share: float,
) -> float:
    """Reg T margin for a short straddle or short strangle.

    Margin = naked margin on the side with the GREATER requirement
             + premium received on the OTHER side.

    Returns margin per contract (× 100).
    """
    call_naked = naked_call_margin(underlying_price, call_strike, call_premium_per_share)
    put_naked = naked_put_margin(underlying_price, put_strike, put_premium_per_share)

    if call_naked >= put_naked:
        # Call side has greater requirement; add put premium
        return call_naked + put_premium_per_share * 100.0
    else:
        # Put side has greater requirement; add call premium
        return put_naked + call_premium_per_share * 100.0


# ---------------------------------------------------------------------------
# Credit spreads (verticals)
# ---------------------------------------------------------------------------


def credit_spread_margin(
    spread_width_per_share: float,
) -> float:
    """Reg T margin for a credit spread (bull put or bear call).

    Margin = spread width (difference between strikes) × 100.
    The credit received reduces capital at risk but not the margin requirement.

    Returns margin per contract (× 100).
    """
    return abs(spread_width_per_share) * 100.0


# ---------------------------------------------------------------------------
# Iron condor / iron butterfly
# ---------------------------------------------------------------------------


def iron_condor_margin(
    call_spread_width_per_share: float,
    put_spread_width_per_share: float,
) -> float:
    """Reg T margin for an iron condor or iron butterfly.

    Margin = the GREATER of the two spread widths × 100.
    Only one side can be in the money at expiration.

    Returns margin per contract (× 100).
    """
    return max(abs(call_spread_width_per_share), abs(put_spread_width_per_share)) * 100.0


# ---------------------------------------------------------------------------
# Covered positions
# ---------------------------------------------------------------------------


def covered_call_margin(underlying_price: float) -> float:
    """Covered call: no option margin. Capital = stock cost.

    Returns capital per contract unit (100 shares × price).
    """
    return underlying_price * 100.0


def cash_secured_put_margin(strike: float) -> float:
    """Cash-secured put: full strike collateral.

    Returns capital per contract (strike × 100).
    """
    return strike * 100.0


def covered_strangle_margin(
    underlying_price: float,
    put_strike: float,
    put_premium_per_share: float,
) -> float:
    """Covered strangle: stock covers the call; naked put margin on the put side.

    Total = stock cost + naked put margin.

    Returns capital per contract unit.
    """
    stock_cost = underlying_price * 100.0
    put_margin = naked_put_margin(underlying_price, put_strike, put_premium_per_share)
    return stock_cost + put_margin


# ---------------------------------------------------------------------------
# Collar
# ---------------------------------------------------------------------------


def collar_margin(underlying_price: float) -> float:
    """Collar: stock cost is the capital requirement. Options offset each other.

    Returns capital per contract unit (100 shares × price).
    """
    return underlying_price * 100.0


# ---------------------------------------------------------------------------
# Jade lizard
# ---------------------------------------------------------------------------


def jade_lizard_margin(
    underlying_price: float,
    put_strike: float,
    put_premium_per_share: float,
    call_spread_width_per_share: float,
    total_credit_per_share: float,
) -> float:
    """Jade lizard: naked put + bear call spread.

    Margin = greater of:
      - naked put margin
      - call spread width × 100
    Less any excess credit over the call spread width (if credit > width,
    upside risk is eliminated).

    Returns margin per contract unit.
    """
    put_naked = naked_put_margin(underlying_price, put_strike, put_premium_per_share)
    call_spread = abs(call_spread_width_per_share) * 100.0
    total_credit = total_credit_per_share * 100.0
    if total_credit >= call_spread:
        return put_naked
    return max(0.0, max(put_naked, call_spread) - total_credit)


# ---------------------------------------------------------------------------
# Ratio backspreads (1:2 short:long)
# ---------------------------------------------------------------------------


def ratio_backspread_margin(
    contract_type: str,
    underlying_price: float,
    short_strike: float,
    short_premium_per_share: float,
) -> float:
    """Ratio backspread (1 short : 2 long): margin on the 1 naked short leg.

    The extra long leg provides unlimited protection on the long side,
    so margin is only on the single short contract.

    Returns margin per contract unit.
    """
    return naked_option_margin(contract_type, underlying_price, short_strike, short_premium_per_share)


# ---------------------------------------------------------------------------
# Short stock positions
# ---------------------------------------------------------------------------


def short_stock_margin(underlying_price: float) -> float:
    """Reg T margin for short stock: 50% of market value + 100% of short sale proceeds.

    Simplified: 150% of underlying × shares, but practically the initial
    margin deposit is 50% of the short value.

    Returns margin per 100 shares.
    """
    return underlying_price * 100.0 * 0.50
