"""Verify entitlement strategy sets cover all entries in the strategy registry."""
from __future__ import annotations


class TestStrategyCoverage:
    def test_all_registry_strategies_in_entitlements(self):
        from backtestforecast.backtests.strategies.registry import STRATEGY_REGISTRY
        from backtestforecast.billing.entitlements import BASIC_STRATEGIES, ADVANCED_STRATEGIES

        all_entitled = BASIC_STRATEGIES | ADVANCED_STRATEGIES
        registry_keys = set(STRATEGY_REGISTRY.keys())

        # Exclude wheel (special engine) and custom leg strategies
        custom_types = {k for k in registry_keys if k.startswith("custom_")}
        check_set = registry_keys - custom_types - {"wheel_strategy"}

        missing = check_set - all_entitled
        assert not missing, (
            f"Strategy types in registry but not in entitlement sets: {missing}. "
            f"Add them to BASIC_STRATEGIES or ADVANCED_STRATEGIES in entitlements.py."
        )
