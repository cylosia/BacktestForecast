from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backtestforecast.config import get_settings
from backtestforecast.schemas.backtests import (
    SYMBOL_ALLOWED_CHARS,
    BacktestSummaryResponse,
    EquityCurvePointResponse,
    RunStatus,
    SpreadWidthMode,
    StrategyType,
)
from backtestforecast.schemas.common import sanitize_error_message
from backtestforecast.schemas.scans import RuleSetDefinition


class SweepMode(str, Enum):
    GRID = "grid"
    GENETIC = "genetic"


class DeltaGridItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: int = Field(ge=1, le=99, description="Target absolute delta (1-99)")


class WidthGridItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: SpreadWidthMode
    value: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def validate_width(self) -> "WidthGridItem":
        if self.mode == SpreadWidthMode.STRIKE_STEPS:
            if self.value < 1 or self.value > 20:
                raise ValueError("strike_steps width must be between 1 and 20")
        if self.mode == SpreadWidthMode.DOLLAR_WIDTH:
            if self.value < Decimal("0.5") or self.value > Decimal("100"):
                raise ValueError("dollar_width must be between 0.50 and 100")
        if self.mode == SpreadWidthMode.PCT_WIDTH:
            if self.value < Decimal("0.5") or self.value > Decimal("30"):
                raise ValueError("pct_width must be between 0.5 and 30")
        return self


class ExitRuleSet(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=120)
    profit_target_pct: float | None = Field(default=None, ge=1.0, le=500.0)
    stop_loss_pct: float | None = Field(default=None, ge=1.0, le=100.0)


class GeneticSweepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    num_legs: int = Field(default=2, ge=2, le=8)
    population_size: int = Field(default=100, ge=20, le=500)
    max_generations: int = Field(default=30, ge=5, le=200)
    tournament_size: int = Field(default=3, ge=2, le=10)
    crossover_rate: float = Field(default=0.7, ge=0.1, le=1.0)
    mutation_rate: float = Field(default=0.3, ge=0.05, le=1.0)
    elitism_count: int = Field(default=5, ge=1, le=50)
    max_workers: int = Field(default=10, ge=1, le=32)
    max_stale_generations: int = Field(default=8, ge=2, le=50)

    @model_validator(mode="after")
    def validate_config(self) -> "GeneticSweepConfig":
        if self.num_legs not in (2, 3, 4, 5, 6, 8):
            raise ValueError("num_legs must be one of 2, 3, 4, 5, 6, or 8")
        if self.elitism_count >= self.population_size:
            raise ValueError("elitism_count must be less than population_size")
        return self


class CreateSweepRequest(BaseModel):
    mode: SweepMode = Field(default=SweepMode.GRID)
    symbol: str = Field(min_length=1, max_length=16)
    strategy_types: list[StrategyType] = Field(min_length=1, max_length=14)
    start_date: date
    end_date: date
    target_dte: int = Field(ge=0, le=365)
    dte_tolerance_days: int = Field(default=5, ge=0, le=60)
    max_holding_days: int = Field(ge=1, le=120)
    account_size: Decimal = Field(gt=0, le=Decimal("100000000"))
    risk_per_trade_pct: Decimal = Field(gt=0, le=100)
    commission_per_contract: Decimal = Field(ge=0, le=Decimal("100"))
    entry_rule_sets: list[RuleSetDefinition] = Field(min_length=1, max_length=10)
    delta_grid: list[DeltaGridItem] = Field(default_factory=list, max_length=20)
    width_grid: list[WidthGridItem] = Field(default_factory=list, max_length=10)
    exit_rule_sets: list[ExitRuleSet] = Field(default_factory=list, max_length=10)
    genetic_config: GeneticSweepConfig | None = Field(default=None)
    max_results: int = Field(default=20, ge=1, le=100)
    slippage_pct: float = Field(default=0.0, ge=0.0, le=5.0)
    idempotency_key: str | None = Field(default=None, min_length=4, max_length=80)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized or any(char not in SYMBOL_ALLOWED_CHARS for char in normalized):
            raise ValueError("symbol must contain only letters, digits, '.', or '-'")
        if not normalized[0].isalpha():
            raise ValueError("symbol must start with a letter")
        return normalized

    @model_validator(mode="after")
    def validate_request(self) -> "CreateSweepRequest":
        from backtestforecast.utils.dates import market_date_today

        if self.end_date > market_date_today():
            raise ValueError("end_date cannot be in the future (US Eastern time).")
        if self.start_date >= self.end_date:
            raise ValueError("start_date must be earlier than end_date")
        if (self.end_date - self.start_date).days < 30:
            raise ValueError("Sweep window must be at least 30 days for meaningful results")
        if (self.end_date - self.start_date).days > get_settings().max_backtest_window_days:
            raise ValueError(
                f"sweep window exceeds the configured maximum of {get_settings().max_backtest_window_days} days"
            )
        if self.dte_tolerance_days >= self.target_dte:
            raise ValueError("dte_tolerance_days must be less than target_dte")
        if len({s.value for s in self.strategy_types}) != len(self.strategy_types):
            raise ValueError("strategy_types must not contain duplicates")
        rule_names = [rs.name.lower() for rs in self.entry_rule_sets]
        if len(set(rule_names)) != len(rule_names):
            raise ValueError("entry_rule_sets must not contain duplicate names")
        if self.exit_rule_sets:
            exit_names = [es.name.lower() for es in self.exit_rule_sets]
            if len(set(exit_names)) != len(exit_names):
                raise ValueError("exit_rule_sets must not contain duplicate names")
        if self.mode == SweepMode.GENETIC and self.genetic_config is None:
            raise ValueError("genetic_config is required when mode is 'genetic'")
        return self


SweepJobStatus = RunStatus


class SweepResultResponse(BaseModel):
    id: UUID
    rank: int
    score: Decimal
    strategy_type: str
    delta: int | None = None
    width_mode: str | None = None
    width_value: Decimal | None = None
    entry_rule_set_name: str
    exit_rule_set_name: str | None = None
    profit_target_pct: float | None = None
    stop_loss_pct: float | None = None
    summary: BacktestSummaryResponse
    warnings: list[dict[str, Any]] = Field(default_factory=list)
    trades_json: list[dict[str, Any]] = Field(default_factory=list)
    equity_curve: list[EquityCurvePointResponse] = Field(default_factory=list)


class SweepResultListResponse(BaseModel):
    items: list[SweepResultResponse]


class SweepJobResponse(BaseModel):
    id: UUID
    status: SweepJobStatus
    symbol: str
    candidate_count: int
    evaluated_candidate_count: int
    result_count: int
    prefetch_summary: dict[str, Any] | None = Field(default=None, alias="prefetch_summary_json")
    warnings: list[dict[str, Any]] = Field(default_factory=list, alias="warnings_json")
    error_code: str | None = None
    error_message: str | None = None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    _sanitize = field_validator("error_message", mode="before")(sanitize_error_message)


class SweepJobListResponse(BaseModel):
    items: list[SweepJobResponse]
    total: int = 0
    offset: int = 0
    limit: int = 50
