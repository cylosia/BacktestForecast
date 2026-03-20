"""Verify template config schema fields are round-trippable."""
from __future__ import annotations

from backtestforecast.schemas.templates import TemplateConfig, TemplateResponse


def test_template_config_has_strategy_type():
    """TemplateConfig must include strategy_type for form restoration."""
    fields = TemplateConfig.model_fields
    assert "strategy_type" in fields
    assert "target_dte" in fields


def test_template_config_includes_reusable_backtest_fields():
    fields = TemplateConfig.model_fields
    for field_name in (
        "slippage_pct",
        "risk_free_rate",
        "profit_target_pct",
        "stop_loss_pct",
        "strategy_overrides",
        "custom_legs",
    ):
        assert field_name in fields


def test_template_response_round_trips_advanced_strategy_fields():
    payload = {
        "id": "00000000-0000-0000-0000-000000000123",
        "name": "Advanced condor",
        "description": "Keeps exits and overrides",
        "strategy_type": "iron_condor",
        "config_json": {
            "strategy_type": "iron_condor",
            "target_dte": 45,
            "dte_tolerance_days": 5,
            "max_holding_days": 12,
            "account_size": "25000",
            "risk_per_trade_pct": "1.5",
            "commission_per_contract": "0.5",
            "entry_rules": [
                {"type": "macd", "fast_period": 8, "slow_period": 21, "signal_period": 5, "direction": "bullish"},
                {"type": "avoid_earnings", "days_before": 4, "days_after": 2},
            ],
            "slippage_pct": "0.2",
            "profit_target_pct": "30",
            "stop_loss_pct": "15",
            "risk_free_rate": "0.038",
            "strategy_overrides": {
                "short_call_strike": {"mode": "delta_target", "value": "20"},
                "short_put_strike": {"mode": "delta_target", "value": "18"},
                "spread_width": {"mode": "dollar_width", "value": "10"},
            },
            "default_symbol": "QQQ",
        },
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-02T00:00:00Z",
    }

    response = TemplateResponse.model_validate(payload)
    dumped = response.model_dump(mode="json")

    assert dumped["config"]["slippage_pct"] == "0.2"
    assert dumped["config"]["profit_target_pct"] == "30"
    assert dumped["config"]["stop_loss_pct"] == "15"
    assert dumped["config"]["risk_free_rate"] == "0.038"
    assert dumped["config"]["strategy_overrides"]["spread_width"]["value"] == "10"


def test_template_response_round_trips_custom_leg_strategies():
    payload = {
        "id": "00000000-0000-0000-0000-000000000124",
        "name": "Custom ladder",
        "description": None,
        "strategy_type": "custom_3_leg",
        "config_json": {
            "strategy_type": "custom_3_leg",
            "target_dte": 60,
            "dte_tolerance_days": 5,
            "max_holding_days": 20,
            "account_size": "10000",
            "risk_per_trade_pct": "2",
            "commission_per_contract": "0.65",
            "entry_rules": [{"type": "rsi", "operator": "lt", "threshold": "30", "period": 14}],
            "custom_legs": [
                {"asset_type": "option", "contract_type": "call", "side": "long", "strike_offset": -1, "expiration_offset": 1, "quantity_ratio": "1"},
                {"asset_type": "option", "contract_type": "call", "side": "short", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": "2"},
                {"asset_type": "stock", "contract_type": None, "side": "long", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": "0.5"},
            ],
            "slippage_pct": "0.1",
            "risk_free_rate": "0.04",
        },
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-02T00:00:00Z",
    }

    response = TemplateResponse.model_validate(payload)
    dumped = response.model_dump(mode="json")

    assert dumped["config"]["custom_legs"][0]["expiration_offset"] == 1
    assert dumped["config"]["custom_legs"][1]["quantity_ratio"] == "2"
    assert dumped["config"]["custom_legs"][2]["asset_type"] == "stock"
