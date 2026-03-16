from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backtestforecast.schemas.backtests import EntryRule, StrategyType, validate_entry_rule_collection


class _Unset(Enum):
    UNSET = "UNSET"


UNSET = _Unset.UNSET


class TemplateConfig(BaseModel):
    """The reusable portion of a backtest configuration — everything except symbol and dates."""

    strategy_type: StrategyType
    target_dte: int = Field(ge=7, le=365)
    dte_tolerance_days: int = Field(default=5, ge=0, le=60)
    max_holding_days: int = Field(ge=1, le=120)
    account_size: Decimal = Field(gt=0, le=Decimal("100000000"))
    risk_per_trade_pct: Decimal = Field(gt=0, le=100)
    commission_per_contract: Decimal = Field(ge=0, le=Decimal("100"))
    entry_rules: list[EntryRule] = Field(default_factory=list, max_length=8)

    # Optional pre-fill hints — not required, but useful if the user always tests the same symbol/window
    default_symbol: str | None = Field(default=None, max_length=16)

    @model_validator(mode="after")
    def validate_template_rules(self) -> "TemplateConfig":
        if self.entry_rules:
            validate_entry_rule_collection(self.entry_rules)
        return self


class CreateTemplateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=500)
    config: TemplateConfig


class UpdateTemplateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None | _Unset = Field(default=UNSET, max_length=500)
    config: TemplateConfig | None = None
    expected_updated_at: datetime | None = None


class TemplateResponse(BaseModel):
    id: UUID
    name: str
    description: str | None
    strategy_type: str
    config: TemplateConfig = Field(alias="config_json")
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def coerce_config(cls, data: Any) -> Any:
        if hasattr(data, "__dict__"):
            raw = getattr(data, "config_json", None)
            if isinstance(raw, dict):
                attrs = {
                    k: getattr(data, k)
                    for k in cls.model_fields
                    if k != "config" and hasattr(data, k)
                }
                attrs["config_json"] = TemplateConfig(**raw)
                return attrs
        elif isinstance(data, dict):
            raw = data.get("config_json") or data.get("config")
            if isinstance(raw, dict):
                data = {**data, "config_json": TemplateConfig(**raw)}
        return data


class TemplateListResponse(BaseModel):
    items: list[TemplateResponse]
    total: int
    template_limit: int | None = None
