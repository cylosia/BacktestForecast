from __future__ import annotations

from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.butterfly import BUTTERFLY_STRATEGY
from backtestforecast.backtests.strategies.calendar import CALENDAR_SPREAD_STRATEGY
from backtestforecast.backtests.strategies.cash_secured_put import CASH_SECURED_PUT_STRATEGY
from backtestforecast.backtests.strategies.collar_strangle import (
    COLLAR_STRATEGY,
    COVERED_STRANGLE_STRATEGY,
)
from backtestforecast.backtests.strategies.covered_call import COVERED_CALL_STRATEGY
from backtestforecast.backtests.strategies.custom import (
    CUSTOM_2_LEG_STRATEGY,
    CUSTOM_3_LEG_STRATEGY,
    CUSTOM_4_LEG_STRATEGY,
    CUSTOM_5_LEG_STRATEGY,
    CUSTOM_6_LEG_STRATEGY,
    CUSTOM_8_LEG_STRATEGY,
)
from backtestforecast.backtests.strategies.diagonal import (
    DIAGONAL_SPREAD_STRATEGY,
    DOUBLE_DIAGONAL_STRATEGY,
    PMCC_STRATEGY,
)
from backtestforecast.backtests.strategies.exotic import (
    IRON_BUTTERFLY_STRATEGY,
    JADE_LIZARD_STRATEGY,
)
from backtestforecast.backtests.strategies.iron_condor import IRON_CONDOR_STRATEGY
from backtestforecast.backtests.strategies.long_options import LONG_CALL_STRATEGY, LONG_PUT_STRATEGY
from backtestforecast.backtests.strategies.naked_options import (
    NAKED_CALL_STRATEGY,
    NAKED_PUT_STRATEGY,
)
from backtestforecast.backtests.strategies.ratio_spreads import (
    RATIO_CALL_BACKSPREAD_STRATEGY,
    RATIO_PUT_BACKSPREAD_STRATEGY,
)
from backtestforecast.backtests.strategies.short_volatility import (
    SHORT_STRADDLE_STRATEGY,
    SHORT_STRANGLE_STRATEGY,
)
from backtestforecast.backtests.strategies.synthetic import (
    REVERSE_CONVERSION_STRATEGY,
    SYNTHETIC_PUT_STRATEGY,
)
from backtestforecast.backtests.strategies.vertical_spreads import (
    BEAR_CALL_CREDIT_SPREAD_STRATEGY,
    BEAR_PUT_DEBIT_SPREAD_STRATEGY,
    BULL_CALL_DEBIT_SPREAD_STRATEGY,
    BULL_PUT_CREDIT_SPREAD_STRATEGY,
)
from backtestforecast.backtests.strategies.volatility import (
    LONG_STRADDLE_STRATEGY,
    LONG_STRANGLE_STRATEGY,
)

STRATEGY_REGISTRY: dict[str, StrategyDefinition] = {
    LONG_CALL_STRATEGY.strategy_type: LONG_CALL_STRATEGY,
    LONG_PUT_STRATEGY.strategy_type: LONG_PUT_STRATEGY,
    COVERED_CALL_STRATEGY.strategy_type: COVERED_CALL_STRATEGY,
    CASH_SECURED_PUT_STRATEGY.strategy_type: CASH_SECURED_PUT_STRATEGY,
    BULL_CALL_DEBIT_SPREAD_STRATEGY.strategy_type: BULL_CALL_DEBIT_SPREAD_STRATEGY,
    BEAR_PUT_DEBIT_SPREAD_STRATEGY.strategy_type: BEAR_PUT_DEBIT_SPREAD_STRATEGY,
    BULL_PUT_CREDIT_SPREAD_STRATEGY.strategy_type: BULL_PUT_CREDIT_SPREAD_STRATEGY,
    BEAR_CALL_CREDIT_SPREAD_STRATEGY.strategy_type: BEAR_CALL_CREDIT_SPREAD_STRATEGY,
    IRON_CONDOR_STRATEGY.strategy_type: IRON_CONDOR_STRATEGY,
    LONG_STRADDLE_STRATEGY.strategy_type: LONG_STRADDLE_STRATEGY,
    LONG_STRANGLE_STRATEGY.strategy_type: LONG_STRANGLE_STRATEGY,
    CALENDAR_SPREAD_STRATEGY.strategy_type: CALENDAR_SPREAD_STRATEGY,
    BUTTERFLY_STRATEGY.strategy_type: BUTTERFLY_STRATEGY,
    SHORT_STRADDLE_STRATEGY.strategy_type: SHORT_STRADDLE_STRATEGY,
    SHORT_STRANGLE_STRATEGY.strategy_type: SHORT_STRANGLE_STRATEGY,
    COLLAR_STRATEGY.strategy_type: COLLAR_STRATEGY,
    COVERED_STRANGLE_STRATEGY.strategy_type: COVERED_STRANGLE_STRATEGY,
    PMCC_STRATEGY.strategy_type: PMCC_STRATEGY,
    DIAGONAL_SPREAD_STRATEGY.strategy_type: DIAGONAL_SPREAD_STRATEGY,
    DOUBLE_DIAGONAL_STRATEGY.strategy_type: DOUBLE_DIAGONAL_STRATEGY,
    RATIO_CALL_BACKSPREAD_STRATEGY.strategy_type: RATIO_CALL_BACKSPREAD_STRATEGY,
    RATIO_PUT_BACKSPREAD_STRATEGY.strategy_type: RATIO_PUT_BACKSPREAD_STRATEGY,
    SYNTHETIC_PUT_STRATEGY.strategy_type: SYNTHETIC_PUT_STRATEGY,
    REVERSE_CONVERSION_STRATEGY.strategy_type: REVERSE_CONVERSION_STRATEGY,
    JADE_LIZARD_STRATEGY.strategy_type: JADE_LIZARD_STRATEGY,
    IRON_BUTTERFLY_STRATEGY.strategy_type: IRON_BUTTERFLY_STRATEGY,
    CUSTOM_2_LEG_STRATEGY.strategy_type: CUSTOM_2_LEG_STRATEGY,
    CUSTOM_3_LEG_STRATEGY.strategy_type: CUSTOM_3_LEG_STRATEGY,
    CUSTOM_4_LEG_STRATEGY.strategy_type: CUSTOM_4_LEG_STRATEGY,
    CUSTOM_5_LEG_STRATEGY.strategy_type: CUSTOM_5_LEG_STRATEGY,
    CUSTOM_6_LEG_STRATEGY.strategy_type: CUSTOM_6_LEG_STRATEGY,
    CUSTOM_8_LEG_STRATEGY.strategy_type: CUSTOM_8_LEG_STRATEGY,
    NAKED_CALL_STRATEGY.strategy_type: NAKED_CALL_STRATEGY,
    NAKED_PUT_STRATEGY.strategy_type: NAKED_PUT_STRATEGY,
}
