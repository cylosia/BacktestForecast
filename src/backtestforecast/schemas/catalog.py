from __future__ import annotations

from pydantic import BaseModel, Field

from backtestforecast.schemas.common import PlanTier


class StrategyCatalogItemResponse(BaseModel):
    strategy_type: str
    label: str
    short_description: str
    category: str
    bias: str
    leg_count: int
    min_tier: PlanTier
    max_loss_description: str
    notes: str = ""
    tags: list[str] = Field(default_factory=list)


class StrategyCatalogGroupResponse(BaseModel):
    category: str
    category_label: str
    strategies: list[StrategyCatalogItemResponse]


class StrategyCatalogResponse(BaseModel):
    groups: list[StrategyCatalogGroupResponse]
    total_strategies: int
    user_tier: PlanTier | None = None
