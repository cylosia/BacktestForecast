from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from backtestforecast.schemas.templates import TemplateConfig


def test_rejects_calendar_override_for_non_calendar_template() -> None:
    with pytest.raises(
        ValidationError,
        match="calendar strategy overrides are only valid for calendar_spread or put_calendar_spread",
    ):
        TemplateConfig(
            strategy_type="long_call",
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("2"),
            commission_per_contract=Decimal("0.65"),
            entry_rules=[],
            strategy_overrides={"calendar_contract_type": "put"},
        )


def test_rejects_far_leg_target_dte_for_non_calendar_template() -> None:
    with pytest.raises(
        ValidationError,
        match="calendar strategy overrides are only valid for calendar_spread or put_calendar_spread",
    ):
        TemplateConfig(
            strategy_type="long_call",
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("2"),
            commission_per_contract=Decimal("0.65"),
            entry_rules=[],
            strategy_overrides={"calendar_far_leg_target_dte": 45},
        )


def test_rejects_far_leg_target_dte_not_greater_than_target_dte() -> None:
    with pytest.raises(
        ValidationError,
        match="calendar_far_leg_target_dte must be greater than target_dte for calendar_spread",
    ):
        TemplateConfig(
            strategy_type="calendar_spread",
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("2"),
            commission_per_contract=Decimal("0.65"),
            entry_rules=[],
            strategy_overrides={"calendar_far_leg_target_dte": 30},
        )


def test_accepts_put_calendar_template_without_contract_override() -> None:
    config = TemplateConfig(
        strategy_type="put_calendar_spread",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("0.65"),
        entry_rules=[],
        strategy_overrides={"calendar_far_leg_target_dte": 45},
    )

    assert config.strategy_type.value == "put_calendar_spread"
    assert config.strategy_overrides is not None
    assert config.strategy_overrides.calendar_far_leg_target_dte == 45
