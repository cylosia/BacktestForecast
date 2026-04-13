from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError as PydanticValidationError

from backtestforecast.schemas.backtests import CreateBacktestRunRequest

COMMON_PAYLOAD = {
    "symbol": "AAPL",
    "strategy_type": "long_call",
    "start_date": date(2025, 1, 1),
    "end_date": date(2025, 2, 1),
    "target_dte": 30,
    "dte_tolerance_days": 5,
    "max_holding_days": 20,
    "account_size": Decimal("10000"),
    "risk_per_trade_pct": Decimal("10"),
    "commission_per_contract": Decimal("1"),
}


def test_conflicting_directional_rules_are_rejected() -> None:
    with pytest.raises(PydanticValidationError):
        CreateBacktestRunRequest(
            **COMMON_PAYLOAD,
            entry_rules=[
                {
                    "type": "ema_crossover",
                    "fast_period": 8,
                    "slow_period": 21,
                    "direction": "bullish",
                },
                {
                    "type": "macd",
                    "direction": "bearish",
                    "fast_period": 12,
                    "slow_period": 26,
                    "signal_period": 9,
                },
            ],
        )


def test_extended_indicator_payload_is_accepted() -> None:
    request = CreateBacktestRunRequest(
        **{**COMMON_PAYLOAD, "strategy_type": "iron_condor"},
        entry_rules=[
            {"type": "iv_rank", "operator": "gte", "threshold": Decimal("50"), "lookback_days": 63},
            {"type": "volume_spike", "operator": "gte", "multiplier": Decimal("1.8"), "lookback_period": 20},
            {
                "type": "support_resistance",
                "mode": "near_resistance",
                "lookback_period": 30,
                "tolerance_pct": Decimal("1.5"),
            },
        ],
    )

    assert request.strategy_type.value == "iron_condor"
    assert len(request.entry_rules) == 3
    assert request.entry_rules[0].type == "iv_rank"


def test_rejects_calendar_override_for_non_calendar_strategy() -> None:
    with pytest.raises(
        PydanticValidationError,
        match="calendar strategy overrides are only valid for calendar_spread or put_calendar_spread",
    ):
        CreateBacktestRunRequest(
            symbol="SPY",
            strategy_type="long_call",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 10),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=10000,
            risk_per_trade_pct=2,
            commission_per_contract=0.65,
            entry_rules=[{"type": "rsi", "operator": "lt", "threshold": 35, "period": 14}],
            strategy_overrides={"calendar_contract_type": "put"},
        )


def test_rejects_far_leg_target_dte_for_non_calendar_strategy() -> None:
    with pytest.raises(
        PydanticValidationError,
        match="calendar strategy overrides are only valid for calendar_spread or put_calendar_spread",
    ):
        CreateBacktestRunRequest(
            symbol="SPY",
            strategy_type="long_call",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 10),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=10000,
            risk_per_trade_pct=2,
            commission_per_contract=0.65,
            entry_rules=[{"type": "rsi", "operator": "lt", "threshold": 35, "period": 14}],
            strategy_overrides={"calendar_far_leg_target_dte": 45},
        )


def test_rejects_far_leg_target_dte_not_greater_than_target_dte() -> None:
    with pytest.raises(
        PydanticValidationError,
        match="calendar_far_leg_target_dte must be greater than target_dte for calendar_spread",
    ):
        CreateBacktestRunRequest(
            symbol="SPY",
            strategy_type="calendar_spread",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 10),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=10000,
            risk_per_trade_pct=2,
            commission_per_contract=0.65,
            entry_rules=[{"type": "rsi", "operator": "lt", "threshold": 35, "period": 14}],
            strategy_overrides={"calendar_far_leg_target_dte": 30},
        )


def test_accepts_put_calendar_spread_without_contract_override() -> None:
    request = CreateBacktestRunRequest(
        symbol="SPY",
        strategy_type="put_calendar_spread",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 10),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=10000,
        risk_per_trade_pct=2,
        commission_per_contract=0.65,
        entry_rules=[{"type": "rsi", "operator": "lt", "threshold": 35, "period": 14}],
        strategy_overrides={"calendar_far_leg_target_dte": 45},
    )

    assert request.strategy_type.value == "put_calendar_spread"
    assert request.strategy_overrides is not None
    assert request.strategy_overrides.calendar_far_leg_target_dte == 45


def test_rejects_call_override_for_put_calendar_spread() -> None:
    with pytest.raises(
        PydanticValidationError,
        match="put_calendar_spread always uses put contracts; calendar_contract_type cannot be set to 'call'",
    ):
        CreateBacktestRunRequest(
            symbol="SPY",
            strategy_type="put_calendar_spread",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 10),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=10000,
            risk_per_trade_pct=2,
            commission_per_contract=0.65,
            entry_rules=[{"type": "rsi", "operator": "lt", "threshold": 35, "period": 14}],
            strategy_overrides={"calendar_contract_type": "call"},
        )
