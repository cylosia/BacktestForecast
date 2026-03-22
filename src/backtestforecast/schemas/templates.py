from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backtestforecast.schemas.backtests import (
    EntryRule,
    StrategyOverrides,
    StrategyType,
    validate_entry_rule_collection,
)

TEMPLATE_SCHEMA_VERSION = 1

# NOTE: Consider migrating to Pydantic's model_fields_set for distinguishing
# "not provided" from "explicitly null" in PATCH operations. The _Unset
# sentinel works but is non-standard.
class _Unset(Enum):
    UNSET = "UNSET"


UNSET = _Unset.UNSET


class TemplateConfig(BaseModel):
    """The reusable portion of a backtest configuration — everything except symbol and dates."""
    model_config = ConfigDict(extra="forbid")

    strategy_type: StrategyType
    target_dte: int = Field(ge=1, le=365)
    dte_tolerance_days: int = Field(default=5, ge=0, le=60)
    max_holding_days: int = Field(ge=1, le=120)
    account_size: Decimal = Field(ge=Decimal("100"), le=Decimal("100000000"))
    risk_per_trade_pct: Decimal = Field(gt=0, le=100)
    commission_per_contract: Decimal = Field(ge=0, le=Decimal("100"))
    # No min_length: empty entry_rules are intentional for template drafts
    # and sweep-style templates that enter on every eligible date.
    entry_rules: list[EntryRule] = Field(default_factory=list, max_length=8)

    # Optional pre-fill hints — not required, but useful if the user always tests the same symbol/window
    default_symbol: str | None = Field(default=None, max_length=16, pattern=r"^[\^A-Z][A-Z0-9./^-]{0,15}$")
    strategy_overrides: StrategyOverrides | None = Field(
        default=None,
        description="Optional strategy-specific overrides such as put-calendar selection.",
    )

    @field_validator("default_symbol", mode="before")
    @classmethod
    def normalize_default_symbol(cls, v: str | None) -> str | None:
        if v is not None:
            return v.strip().upper()
        return v

    @model_validator(mode="after")
    def validate_template_rules(self) -> "TemplateConfig":
        if self.dte_tolerance_days >= self.target_dte:
            raise ValueError("dte_tolerance_days must be less than target_dte")
        if self.entry_rules:
            validate_entry_rule_collection(self.entry_rules)
        return self


class CreateTemplateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=2000)
    config: TemplateConfig


class UpdateTemplateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None | _Unset = Field(default=UNSET, max_length=2000)
    config: TemplateConfig | None = None
    expected_updated_at: datetime | None = None


class TemplateResponse(BaseModel):
    id: UUID
    name: str
    description: str | None
    strategy_type: str
    config: TemplateConfig = Field(validation_alias="config_json")
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def coerce_config(cls, data: Any) -> Any:
        if hasattr(data, "__tablename__"):
            raw = getattr(data, "config_json", None)
            if isinstance(raw, dict) and raw:
                attrs = {
                    k: getattr(data, k)
                    for k in cls.model_fields
                    if k != "config" and hasattr(data, k)
                }
                attrs["config_json"] = TemplateConfig(**raw)
                return attrs
            if isinstance(raw, dict) and not raw:
                raise ValueError("Template has empty config_json; cannot be serialized.")
        elif isinstance(data, dict):
            raw = data.get("config_json") or data.get("config")
            if isinstance(raw, dict) and raw:
                data = {**data, "config_json": TemplateConfig(**raw)}
        return data


class TemplateListResponse(BaseModel):
    items: list[TemplateResponse]
    total: int
    template_limit: int | None = None
