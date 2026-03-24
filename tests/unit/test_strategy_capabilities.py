from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest


@pytest.mark.parametrize("strategy_type", ["custom_2_leg", "custom_7_leg"])
def test_scan_request_rejects_custom_strategies_without_user_defined_context(strategy_type: str) -> None:
    from backtestforecast.schemas.backtests import RsiRule
    from backtestforecast.schemas.scans import CreateScannerJobRequest, RuleSetDefinition

    with pytest.raises(ValueError, match="Scanner jobs do not support"):
        CreateScannerJobRequest(
            symbols=["AAPL"],
            strategy_types=[strategy_type],
            rule_sets=[RuleSetDefinition(name="rsi", entry_rules=[RsiRule(type="rsi", operator="lte", threshold=Decimal("40"))])],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 3, 15),
            target_dte=30,
            max_holding_days=20,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("2"),
            commission_per_contract=Decimal("1"),
        )


def test_grid_sweep_request_rejects_custom_and_multi_cycle_strategies() -> None:
    from backtestforecast.schemas.backtests import RsiRule
    from backtestforecast.schemas.scans import RuleSetDefinition
    from backtestforecast.schemas.sweeps import CreateSweepRequest

    with pytest.raises(ValueError, match="Grid sweeps do not support"):
        CreateSweepRequest(
            symbol="AAPL",
            strategy_types=["custom_2_leg", "wheel_strategy"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 3, 15),
            target_dte=30,
            max_holding_days=20,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("2"),
            commission_per_contract=Decimal("1"),
            entry_rule_sets=[RuleSetDefinition(name="rsi", entry_rules=[RsiRule(type="rsi", operator="lte", threshold=Decimal("40"))])],
        )
