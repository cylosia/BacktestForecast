from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from backtestforecast.schemas.templates import TemplateConfig


def test_rejects_calendar_override_for_non_calendar_template() -> None:
    with pytest.raises(ValidationError, match="calendar_contract_type override is only valid for calendar_spread"):
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
