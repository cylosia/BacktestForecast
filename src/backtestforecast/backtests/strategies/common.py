from __future__ import annotations

import math
from collections import defaultdict
from datetime import date
from typing import Iterable

from backtestforecast.errors import DataUnavailableError
from backtestforecast.market_data.types import OptionContractRecord
from backtestforecast.schemas.backtests import (
    SpreadWidthConfig,
    SpreadWidthMode,
    StrategyOverrides,
    StrikeSelection,
    StrikeSelectionMode,
)


def group_contracts_by_expiration(contracts: Iterable[OptionContractRecord]) -> dict[date, list[OptionContractRecord]]:
    grouped: dict[date, list[OptionContractRecord]] = defaultdict(list)
    for contract in contracts:
        grouped[contract.expiration_date].append(contract)
    return grouped


def choose_primary_expiration(
    contracts: Iterable[OptionContractRecord],
    entry_date: date,
    target_dte: int,
) -> date:
    expirations = {contract.expiration_date for contract in contracts}
    if not expirations:
        raise DataUnavailableError("No eligible option expirations were available.")
    return min(
        expirations,
        key=lambda expiration: (
            abs((expiration - entry_date).days - target_dte),
            0 if (expiration - entry_date).days >= target_dte else 1,
            (expiration - entry_date).days,
        ),
    )


def choose_secondary_expiration(
    contracts: Iterable[OptionContractRecord],
    entry_date: date,
    base_expiration: date,
    min_extra_days: int = 14,
) -> date | None:
    expirations = sorted(
        {contract.expiration_date for contract in contracts if contract.expiration_date > base_expiration}
    )
    if not expirations:
        return None
    minimum_target = (base_expiration - entry_date).days + min_extra_days
    later_candidates = [expiration for expiration in expirations if (expiration - entry_date).days >= minimum_target]
    if later_candidates:
        return later_candidates[0]
    return expirations[0]


def contracts_for_expiration(contracts: Iterable[OptionContractRecord], expiration: date) -> list[OptionContractRecord]:
    return [contract for contract in contracts if contract.expiration_date == expiration]


def sorted_unique_strikes(contracts: Iterable[OptionContractRecord]) -> list[float]:
    return sorted({contract.strike_price for contract in contracts})


def choose_atm_strike(strikes: list[float], underlying_close: float) -> float:
    if not strikes:
        raise DataUnavailableError("No strikes were available for the selected expiration.")
    return min(strikes, key=lambda strike: (abs(strike - underlying_close), strike))


def choose_call_otm_strike(strikes: list[float], underlying_close: float) -> float:
    above = [strike for strike in strikes if strike >= underlying_close]
    if above:
        return min(above)
    return max(strikes)


def choose_put_otm_strike(strikes: list[float], underlying_close: float) -> float:
    below = [strike for strike in strikes if strike <= underlying_close]
    if below:
        return max(below)
    return min(strikes)


def offset_strike(strikes: list[float], base_strike: float, steps: int) -> float | None:
    ordered = sorted(strikes)
    if base_strike not in ordered:
        ordered.append(base_strike)
        ordered.sort()
    index = ordered.index(base_strike)
    target_index = index + steps
    if target_index < 0 or target_index >= len(ordered):
        return None
    return ordered[target_index]


def require_contract_for_strike(contracts: Iterable[OptionContractRecord], strike: float) -> OptionContractRecord:
    for contract in contracts:
        if contract.strike_price == strike:
            return contract
    raise DataUnavailableError(f"No contract was available for strike {strike}.")


def choose_common_atm_strike(
    call_contracts: Iterable[OptionContractRecord],
    put_contracts: Iterable[OptionContractRecord],
    underlying_close: float,
) -> float:
    common_strikes = sorted(
        {contract.strike_price for contract in call_contracts} & {contract.strike_price for contract in put_contracts}
    )
    if not common_strikes:
        raise DataUnavailableError("No common call/put strike was available for the selected expiration.")
    return choose_atm_strike(common_strikes, underlying_close)


def intrinsic_value(contract_type: str, strike_price: float, underlying_close: float) -> float:
    if contract_type == "call":
        return max(0.0, underlying_close - strike_price)
    return max(0.0, strike_price - underlying_close)


def synthetic_ticker(identifiers: list[str]) -> str:
    return "|".join(identifiers)


# ---------------------------------------------------------------------------
# Configurable strike resolution
# ---------------------------------------------------------------------------


def _approx_bsm_delta(
    spot: float,
    strike: float,
    dte_days: int,
    contract_type: str,
    vol: float = 0.30,
    risk_free_rate: float = 0.045,
) -> float:
    """Approximate Black-Scholes delta without provider greeks.

    Uses a default 30% annualized vol assumption.  Sufficient for
    strike-selection targeting, not for pricing.
    """
    if dte_days <= 0:
        if contract_type == "call":
            return 1.0 if spot > strike else 0.0
        return -1.0 if spot < strike else 0.0

    t = dte_days / 365.0
    sqrt_t = math.sqrt(t)
    try:
        d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * vol * vol) * t) / (vol * sqrt_t)
    except (ValueError, ZeroDivisionError):
        return 0.5 if contract_type == "call" else -0.5

    # Standard normal CDF approximation (Abramowitz & Stegun)
    def _norm_cdf(x: float) -> float:
        a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
        p = 0.3275911
        sign = 1.0 if x >= 0 else -1.0
        x_abs = abs(x)
        t_ = 1.0 / (1.0 + p * x_abs)
        y = 1.0 - (((((a5 * t_ + a4) * t_) + a3) * t_ + a2) * t_ + a1) * t_ * math.exp(-x_abs * x_abs / 2.0)
        return 0.5 * (1.0 + sign * y)

    if contract_type == "call":
        return _norm_cdf(d1)
    return _norm_cdf(d1) - 1.0


def _nearest_strike(strikes: list[float], target: float) -> float:
    """Find the listed strike closest to a target value."""
    if not strikes:
        raise DataUnavailableError("No strikes available.")
    return min(strikes, key=lambda s: (abs(s - target), s))


def resolve_strike(
    strikes: list[float],
    underlying_close: float,
    contract_type: str,
    selection: StrikeSelection | None,
    dte_days: int = 30,
) -> float:
    """Resolve a strike based on the selection config, or fall back to nearest OTM."""
    if selection is None or selection.mode == StrikeSelectionMode.NEAREST_OTM:
        if contract_type == "call":
            return choose_call_otm_strike(strikes, underlying_close)
        return choose_put_otm_strike(strikes, underlying_close)

    val = float(selection.value) if selection.value is not None else 0.0

    if selection.mode == StrikeSelectionMode.PCT_FROM_SPOT:
        if contract_type == "call":
            target = underlying_close * (1.0 + val / 100.0)
        else:
            target = underlying_close * (1.0 - val / 100.0)
        return _nearest_strike(strikes, target)

    if selection.mode == StrikeSelectionMode.ATM_OFFSET_STEPS:
        steps = int(val)
        atm = choose_atm_strike(strikes, underlying_close)
        sorted_strikes = sorted(set(strikes))
        if contract_type == "call":
            resolved = offset_strike(sorted_strikes, atm, steps)
        else:
            resolved = offset_strike(sorted_strikes, atm, -steps)
        if resolved is None:
            raise DataUnavailableError(f"Strike offset {steps} out of range for {contract_type}.")
        return resolved

    if selection.mode == StrikeSelectionMode.DELTA_TARGET:
        target_delta = val / 100.0  # User provides 30 for 30Δ
        best_strike = strikes[0]
        best_diff = float("inf")
        for strike in strikes:
            delta = _approx_bsm_delta(underlying_close, strike, dte_days, contract_type)
            diff = abs(abs(delta) - target_delta)
            if diff < best_diff:
                best_diff = diff
                best_strike = strike
        return best_strike

    # Fallback
    if contract_type == "call":
        return choose_call_otm_strike(strikes, underlying_close)
    return choose_put_otm_strike(strikes, underlying_close)


def resolve_wing_strike(
    strikes: list[float],
    short_strike: float,
    direction: int,
    underlying_close: float,
    width_config: SpreadWidthConfig | None,
) -> float | None:
    """Resolve a wing/protection strike relative to a short strike.

    Args:
        strikes: Available listed strikes.
        short_strike: The anchor (short leg) strike.
        direction: +1 for higher (call wing), -1 for lower (put wing).
        underlying_close: Current underlying price (for pct_width).
        width_config: Optional spread width configuration.

    Returns:
        The resolved wing strike, or None if no valid strike exists.
    """
    if width_config is None:
        # Default: one listed-strike increment
        return offset_strike(sorted(set(strikes)), short_strike, direction)

    val = float(width_config.value)

    if width_config.mode == SpreadWidthMode.STRIKE_STEPS:
        steps = int(val)
        return offset_strike(sorted(set(strikes)), short_strike, direction * steps)

    if width_config.mode == SpreadWidthMode.DOLLAR_WIDTH:
        if direction > 0:
            target = short_strike + val
        else:
            target = short_strike - val
        return _nearest_strike(strikes, target)

    if width_config.mode == SpreadWidthMode.PCT_WIDTH:
        dollar_width = underlying_close * val / 100.0
        if direction > 0:
            target = short_strike + dollar_width
        else:
            target = short_strike - dollar_width
        return _nearest_strike(strikes, target)

    # Fallback
    return offset_strike(sorted(set(strikes)), short_strike, direction)


def get_overrides(config_overrides: StrategyOverrides | None) -> StrategyOverrides:
    """Return the overrides or an empty default."""
    if config_overrides is not None:
        return config_overrides
    return StrategyOverrides()
