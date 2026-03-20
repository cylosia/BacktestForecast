"""Verify template config schema fields are round-trippable."""
from __future__ import annotations


def test_template_config_has_strategy_type():
    """TemplateConfig must include strategy_type for form restoration."""
    from backtestforecast.schemas.templates import TemplateConfig
    fields = TemplateConfig.model_fields
    assert "strategy_type" in fields
    assert "target_dte" in fields


def test_template_config_has_calendar_contract_type():
    from backtestforecast.schemas.templates import TemplateConfig
    fields = TemplateConfig.model_fields
    assert "calendar_contract_type" in fields
