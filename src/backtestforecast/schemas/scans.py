from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backtestforecast.billing.entitlements import ScannerMode
from backtestforecast.config import get_settings
from backtestforecast.schemas.backtests import (
    SYMBOL_ALLOWED_CHARS,
    BacktestSummaryResponse,
    BacktestTradeResponse,
    EntryRule,
    EquityCurvePointResponse,
    StrategyType,
    validate_entry_rule_collection,
)


class ScannerJobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RuleSetDefinition(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    entry_rules: list[EntryRule] = Field(default_factory=list, max_length=8)

    @field_validator("name", mode="before")
    @classmethod
    def normalize_name(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value

    @model_validator(mode="after")
    def validate_rule_set(self) -> "RuleSetDefinition":
        validate_entry_rule_collection(self.entry_rules)
        return self


class CreateScannerJobRequest(BaseModel):
    name: str | None = Field(default=None, max_length=120)
    mode: ScannerMode = Field(default=ScannerMode.BASIC)
    symbols: list[str] = Field(min_length=1, max_length=25)
    strategy_types: list[StrategyType] = Field(min_length=1, max_length=14)
    rule_sets: list[RuleSetDefinition] = Field(min_length=1, max_length=10)
    start_date: date
    end_date: date
    target_dte: int = Field(ge=7, le=365)
    dte_tolerance_days: int = Field(default=5, ge=0, le=60)
    max_holding_days: int = Field(ge=1, le=120)
    account_size: Decimal = Field(gt=0, le=Decimal("100000000"))
    risk_per_trade_pct: Decimal = Field(gt=0, le=100)
    commission_per_contract: Decimal = Field(ge=0, le=Decimal("100"))
    max_recommendations: int = Field(default=10, ge=1, le=30)
    refresh_daily: bool = False
    refresh_priority: int = Field(default=50, ge=0, le=100)
    idempotency_key: str | None = Field(default=None, min_length=4, max_length=80)

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for symbol in value:
            candidate = symbol.strip().upper()
            if not candidate:
                continue
            if any(char not in SYMBOL_ALLOWED_CHARS for char in candidate):
                raise ValueError("symbols must contain only letters, digits, '.', or '-'")
            if not candidate[0].isalpha():
                raise ValueError("symbols must start with a letter")
            if candidate in seen:
                continue
            seen.add(candidate)
            normalized.append(candidate)
        if not normalized:
            raise ValueError("At least one symbol is required")
        return normalized

    @model_validator(mode="after")
    def validate_request(self) -> "CreateScannerJobRequest":
        if self.end_date > datetime.now(UTC).date() + timedelta(days=1):
            raise ValueError("end_date cannot be in the future")
        if self.start_date >= self.end_date:
            raise ValueError("start_date must be earlier than end_date")
        if (self.end_date - self.start_date).days > get_settings().max_scanner_window_days:
            raise ValueError(
                f"scanner window exceeds the configured maximum of {get_settings().max_scanner_window_days} days"
            )
        if len({strategy.value for strategy in self.strategy_types}) != len(self.strategy_types):
            raise ValueError("strategy_types must not contain duplicates")
        rule_names = [rule_set.name.lower() for rule_set in self.rule_sets]
        if len(set(rule_names)) != len(rule_names):
            raise ValueError("rule_sets must not contain duplicate names")
        return self


class HistoricalPerformanceResponse(BaseModel):
    sample_count: int = 0
    weighted_win_rate: Decimal = Decimal("0")
    weighted_total_roi_pct: Decimal = Decimal("0")
    weighted_total_net_pnl: Decimal = Decimal("0")
    weighted_max_drawdown_pct: Decimal = Decimal("0")
    recency_half_life_days: int = 180
    last_observed_at: datetime | None = None


class HistoricalAnalogForecastResponse(BaseModel):
    symbol: str
    strategy_type: str | None = None
    as_of_date: date
    horizon_days: int
    analog_count: int
    expected_return_low_pct: Decimal
    expected_return_median_pct: Decimal
    expected_return_high_pct: Decimal
    positive_outcome_rate_pct: Decimal
    summary: str
    disclaimer: str
    analog_dates: list[date] = Field(default_factory=list)


class RankingBreakdownResponse(BaseModel):
    current_performance_score: Decimal
    historical_performance_score: Decimal
    forecast_alignment_score: Decimal
    final_score: Decimal
    reasoning: list[str] = Field(default_factory=list)


class ScannerRecommendationResponse(BaseModel):
    id: UUID
    rank: int
    score: Decimal
    symbol: str
    strategy_type: str
    rule_set_name: str
    request_snapshot: dict[str, Any]
    summary: BacktestSummaryResponse
    warnings: list[dict[str, Any]] = Field(default_factory=list)
    historical_performance: HistoricalPerformanceResponse
    forecast: HistoricalAnalogForecastResponse
    ranking_breakdown: RankingBreakdownResponse
    trades: list[BacktestTradeResponse] = Field(default_factory=list)
    equity_curve: list[EquityCurvePointResponse] = Field(default_factory=list)


class ScannerRecommendationListResponse(BaseModel):
    items: list[ScannerRecommendationResponse]


class ScannerJobResponse(BaseModel):
    id: UUID
    name: str | None
    status: ScannerJobStatus
    mode: ScannerMode
    plan_tier_snapshot: str
    job_kind: str
    candidate_count: int
    evaluated_candidate_count: int
    recommendation_count: int
    refresh_daily: bool
    refresh_priority: int
    warnings: list[dict[str, Any]] = Field(default_factory=list, alias="warnings_json")
    error_code: str | None = None
    error_message: str | None = None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class ScannerJobListResponse(BaseModel):
    items: list[ScannerJobResponse]
