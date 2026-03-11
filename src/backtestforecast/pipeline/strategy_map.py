"""Strategy-regime mapping for the nightly scan pipeline.

Maps regime combinations to the strategies most likely to perform well.
This is a configuration table, not code — tunable over time.
"""

from __future__ import annotations

from backtestforecast.pipeline.regime import Regime

# Each key is a tuple of (directional_regime, volatility_regime).
# Values are lists of strategy_type strings ordered by priority.

REGIME_STRATEGY_MAP: dict[tuple[Regime, Regime | None], list[str]] = {
    # --- Bullish ---
    (Regime.BULLISH, Regime.LOW_IV): [
        "long_call",
        "bull_call_debit_spread",
        "poor_mans_covered_call",
        "diagonal_spread",
    ],
    (Regime.BULLISH, Regime.HIGH_IV): [
        "bull_put_credit_spread",
        "cash_secured_put",
        "jade_lizard",
        "covered_call",
        "collar",
    ],
    (Regime.BULLISH, None): [
        "long_call",
        "bull_call_debit_spread",
        "covered_call",
        "cash_secured_put",
    ],
    # --- Bearish ---
    (Regime.BEARISH, Regime.LOW_IV): [
        "long_put",
        "bear_put_debit_spread",
        "ratio_put_backspread",
        "synthetic_put",
    ],
    (Regime.BEARISH, Regime.HIGH_IV): [
        "bear_call_credit_spread",
        "naked_put",
        "bear_put_debit_spread",
    ],
    (Regime.BEARISH, None): [
        "long_put",
        "bear_put_debit_spread",
        "bear_call_credit_spread",
    ],
    # --- Neutral ---
    (Regime.NEUTRAL, Regime.HIGH_IV): [
        "iron_condor",
        "iron_butterfly",
        "short_strangle",
        "short_straddle",
        "covered_strangle",
        "jade_lizard",
    ],
    (Regime.NEUTRAL, Regime.LOW_IV): [
        "long_straddle",
        "long_strangle",
        "ratio_call_backspread",
        "calendar_spread",
        "double_diagonal",
    ],
    (Regime.NEUTRAL, None): [
        "iron_condor",
        "butterfly",
        "calendar_spread",
        "covered_call",
    ],
}


# Default parameter grids for quick-backtest sampling (Stage 3).
# Each config is a dict of overrides to merge into the base request.
DEFAULT_PARAM_GRID: list[dict[str, object]] = [
    {"target_dte": 30, "strategy_overrides": None},
    {"target_dte": 45, "strategy_overrides": None},
    {
        "target_dte": 30,
        "strategy_overrides": {
            "short_call_strike": {"mode": "delta_target", "value": 30},
            "short_put_strike": {"mode": "delta_target", "value": 30},
        },
    },
    {
        "target_dte": 45,
        "strategy_overrides": {
            "short_call_strike": {"mode": "delta_target", "value": 16},
            "short_put_strike": {"mode": "delta_target", "value": 16},
        },
    },
    {
        "target_dte": 30,
        "strategy_overrides": {
            "spread_width": {"mode": "dollar_width", "value": 5},
        },
    },
]


def strategies_for_regime(
    regimes: frozenset[Regime],
) -> list[str]:
    """Return the list of strategy types appropriate for the given regime set."""
    # Extract directional and volatility components
    directional = Regime.NEUTRAL
    for r in (Regime.BULLISH, Regime.BEARISH, Regime.NEUTRAL):
        if r in regimes:
            directional = r
            break

    volatility: Regime | None = None
    if Regime.HIGH_IV in regimes:
        volatility = Regime.HIGH_IV
    elif Regime.LOW_IV in regimes:
        volatility = Regime.LOW_IV

    # Look up exact match first, then fallback to no-volatility match
    strategies = REGIME_STRATEGY_MAP.get((directional, volatility))
    if strategies is None:
        strategies = REGIME_STRATEGY_MAP.get((directional, None), [])

    return list(strategies)
