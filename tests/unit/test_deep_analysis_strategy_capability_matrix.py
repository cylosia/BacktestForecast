from __future__ import annotations


def test_deep_analysis_capability_matrix_matches_strategy_expectations() -> None:
    from backtestforecast.backtests.strategies.registry import STRATEGY_REGISTRY
    from backtestforecast.strategy_catalog.capabilities import (
        get_strategy_capabilities,
        list_symbol_analysis_strategy_types,
    )

    discovered = set(list_symbol_analysis_strategy_types(list(STRATEGY_REGISTRY.keys())))

    registered_but_unsupported_without_user_defined_context = {
        "custom_2_leg",
        "custom_3_leg",
        "custom_4_leg",
        "custom_5_leg",
        "custom_6_leg",
        "custom_7_leg",
        "custom_8_leg",
    }
    unsupported_outside_registry = {"wheel_strategy"}
    supported_examples = {
        "long_call",
        "naked_call",
        "iron_condor",
        "calendar_spread",
        "jade_lizard",
    }

    assert registered_but_unsupported_without_user_defined_context.issubset(STRATEGY_REGISTRY.keys())
    for strategy_type in registered_but_unsupported_without_user_defined_context:
        capabilities = get_strategy_capabilities(strategy_type)
        assert capabilities.requires_user_defined_context
        assert not capabilities.supports_symbol_analysis
        assert strategy_type not in discovered
    for strategy_type in unsupported_outside_registry:
        assert not get_strategy_capabilities(strategy_type).supports_symbol_analysis
        assert strategy_type not in discovered
    for strategy_type in supported_examples:
        assert strategy_type in STRATEGY_REGISTRY, strategy_type
        assert get_strategy_capabilities(strategy_type).supports_symbol_analysis, strategy_type
        assert strategy_type in discovered, strategy_type
