from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backtestforecast.schemas.backtests import (
    CustomLegDefinition,
    EntryRule,
    StrategyOverrides,
    StrategyType,
    CUSTOM_LEG_COUNT,
    CUSTOM_STRATEGY_TYPES,
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
    custom_legs: list[CustomLegDefinition] | None = Field(default=None, max_length=8)
    slippage_pct: Decimal = Field(default=Decimal("0"), ge=Decimal("0"), le=Decimal("5"))
    profit_target_pct: Decimal | None = Field(default=None, ge=Decimal("1"), le=Decimal("500"))
    stop_loss_pct: Decimal | None = Field(default=None, ge=Decimal("1"), le=Decimal("100"))
    strategy_overrides: StrategyOverrides | None = None
    risk_free_rate: Decimal | None = Field(default=None, ge=Decimal("0"), le=Decimal("0.20"))

    # Optional pre-fill hints — not required, but useful if the user always tests the same symbol/window
    default_symbol: str | None = Field(default=None, max_length=16, pattern=r"^[\^A-Z][A-Z0-9./^-]{0,15}$")

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
        if self.strategy_type in CUSTOM_STRATEGY_TYPES:
            expected = CUSTOM_LEG_COUNT[self.strategy_type]
            if not self.custom_legs:
                raise ValueError(f"{self.strategy_type.value} requires exactly {expected} custom_legs definitions")
            if len(self.custom_legs) != expected:
                raise ValueError(f"{self.strategy_type.value} requires exactly {expected} legs, got {len(self.custom_legs)}")
        elif self.custom_legs:
            raise ValueError("custom_legs should only be provided for custom_N_leg strategy types")

        if self.custom_legs:
            long_count = sum(1 for leg in self.custom_legs if leg.side == "long")
            short_count = sum(1 for leg in self.custom_legs if leg.side == "short")
            if long_count == 0 or short_count == 0:
                raise ValueError("custom_legs must contain at least one long and one short leg")
        return self


class CreateTemplateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=500)
    config: TemplateConfig


class UpdateTemplateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None | _Unset = Field(default=UNSET, max_length=500)
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
