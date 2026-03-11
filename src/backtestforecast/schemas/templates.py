from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from backtestforecast.schemas.backtests import EntryRule, StrategyType


class TemplateConfig(BaseModel):
    """The reusable portion of a backtest configuration — everything except symbol and dates."""

    strategy_type: StrategyType
    target_dte: int = Field(ge=7, le=365)
    dte_tolerance_days: int = Field(default=5, ge=0, le=60)
    max_holding_days: int = Field(ge=1, le=120)
    account_size: Decimal = Field(gt=0)
    risk_per_trade_pct: Decimal = Field(gt=0, le=100)
    commission_per_contract: Decimal = Field(ge=0)
    entry_rules: list[EntryRule] = Field(default_factory=list, max_length=8)

    # Optional pre-fill hints — not required, but useful if the user always tests the same symbol/window
    default_symbol: str | None = Field(default=None, max_length=16)


class CreateTemplateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=500)
    config: TemplateConfig


class UpdateTemplateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=500)
    config: TemplateConfig | None = None


class TemplateResponse(BaseModel):
    id: UUID
    name: str
    description: str | None
    strategy_type: str
    config: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TemplateListResponse(BaseModel):
    items: list[TemplateResponse]
    total: int
    template_limit: int | None = None
