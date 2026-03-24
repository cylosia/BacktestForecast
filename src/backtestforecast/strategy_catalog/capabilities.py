from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.schemas.backtests import CUSTOM_STRATEGY_TYPES, StrategyType


@dataclass(frozen=True, slots=True)
class StrategyCapabilities:
    strategy_type: str
    requires_user_defined_context: bool = False
    supports_symbol_analysis: bool = True
    supports_scans: bool = True
    supports_grid_sweeps: bool = True


_CUSTOM_STRATEGY_VALUES = frozenset(strategy.value for strategy in CUSTOM_STRATEGY_TYPES)
_CAPABILITIES: dict[str, StrategyCapabilities] = {
    strategy.value: StrategyCapabilities(strategy_type=strategy.value)
    for strategy in StrategyType
}

for strategy_type in _CUSTOM_STRATEGY_VALUES:
    _CAPABILITIES[strategy_type] = StrategyCapabilities(
        strategy_type=strategy_type,
        requires_user_defined_context=True,
        supports_symbol_analysis=False,
        supports_scans=False,
        supports_grid_sweeps=False,
    )

_CAPABILITIES[StrategyType.WHEEL.value] = StrategyCapabilities(
    strategy_type=StrategyType.WHEEL.value,
    supports_symbol_analysis=False,
    supports_grid_sweeps=False,
)


def get_strategy_capabilities(strategy_type: str) -> StrategyCapabilities:
    return _CAPABILITIES.get(strategy_type, StrategyCapabilities(strategy_type=strategy_type))


def list_symbol_analysis_strategy_types(strategy_types: list[str]) -> list[str]:
    return [strategy_type for strategy_type in strategy_types if get_strategy_capabilities(strategy_type).supports_symbol_analysis]


def unsupported_scan_strategies(strategy_types: list[str]) -> list[str]:
    return [strategy_type for strategy_type in strategy_types if not get_strategy_capabilities(strategy_type).supports_scans]


def unsupported_grid_sweep_strategies(strategy_types: list[str]) -> list[str]:
    return [strategy_type for strategy_type in strategy_types if not get_strategy_capabilities(strategy_type).supports_grid_sweeps]
