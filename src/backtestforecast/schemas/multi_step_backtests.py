from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backtestforecast.schemas.backtests import (
    SYMBOL_ALLOWED_CHARS,
    BacktestSummaryResponse,
    CustomLegDefinition,
    EntryRule,
    EquityCurvePointResponse,
    StrategyOverrides,
    StrategyType,
    validate_entry_rule_collection,
)
from backtestforecast.schemas.common import CursorPaginatedResponse, RunJobStatus, WarningResponse


class StepTriggerDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["date_offset", "after_expiration", "rule_match", "event_and_rule"]
    days_after_prior_step: int | None = Field(default=None, ge=0, le=365)
    rules: list[EntryRule] = Field(default_factory=list, max_length=8)
    require_prior_step_status: Literal["filled", "expired", "closed"] | None = None

    @model_validator(mode="after")
    def validate_trigger(self) -> StepTriggerDefinition:
        if self.mode == "date_offset" and self.days_after_prior_step is None:
            raise ValueError("days_after_prior_step is required for date_offset")
        if self.mode in {"rule_match", "event_and_rule"} and not self.rules:
            raise ValueError("rules are required for rule-based triggers")
        validate_entry_rule_collection(self.rules)
        return self


class StepContractSelection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy_type: StrategyType
    target_dte: int = Field(ge=1, le=365)
    dte_tolerance_days: int = Field(default=5, ge=0, le=60)
    max_holding_days: int = Field(ge=1, le=120)
    custom_legs: list[CustomLegDefinition] | None = Field(default=None, max_length=8)
    strategy_overrides: StrategyOverrides | None = None


class WorkflowStepDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    step_number: int = Field(ge=1, le=20)
    name: str = Field(min_length=1, max_length=120)
    action: Literal["open_position", "sell_premium", "roll", "close_position", "hedge"]
    trigger: StepTriggerDefinition
    contract_selection: StepContractSelection
    failure_policy: Literal["liquidate"] = "liquidate"


class CreateMultiStepRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, max_length=120)
    symbol: str = Field(min_length=1, max_length=16)
    workflow_type: str = Field(min_length=1, max_length=80)
    start_date: date
    end_date: date
    account_size: Decimal = Field(ge=Decimal("100"), le=Decimal("100000000"))
    risk_per_trade_pct: Decimal = Field(gt=0, le=100)
    commission_per_contract: Decimal = Field(ge=0, le=Decimal("100"))
    slippage_pct: Decimal = Field(default=Decimal("0"), ge=Decimal("0"), le=Decimal("5"))
    initial_entry_rules: list[EntryRule] = Field(min_length=1, max_length=8)
    steps: list[WorkflowStepDefinition] = Field(min_length=2, max_length=20)
    idempotency_key: str | None = Field(default=None, min_length=4, max_length=80)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not SYMBOL_ALLOWED_CHARS.match(normalized):
            raise ValueError("Invalid symbol")
        return normalized

    @model_validator(mode="after")
    def validate_steps(self) -> CreateMultiStepRunRequest:
        from backtestforecast.utils.dates import market_date_today

        if self.end_date > market_date_today():
            raise ValueError("end_date cannot be in the future (US Eastern time).")
        if self.start_date >= self.end_date:
            raise ValueError("start_date must be earlier than end_date")

        validate_entry_rule_collection(self.initial_entry_rules)
        ordered = sorted(step.step_number for step in self.steps)
        if ordered != list(range(1, len(self.steps) + 1)):
            raise ValueError("steps must be consecutively numbered starting from 1")
        return self


class MultiStepEventResponse(BaseModel):
    step_number: int
    event_type: Literal["triggered", "filled", "skipped", "failed", "liquidated"]
    event_at: datetime
    message: str | None = None
    payload_json: dict[str, Any] = Field(default_factory=dict)


class MultiStepStepOutcomeResponse(BaseModel):
    step_number: int
    name: str
    action: str
    status: Literal["pending", "waiting", "executed", "failed", "liquidated", "skipped"]
    triggered_at: datetime | None = None
    executed_at: datetime | None = None
    failure_reason: str | None = None


class MultiStepTradeResponse(BaseModel):
    id: UUID
    step_number: int
    option_ticker: str
    strategy_type: str
    entry_date: date
    exit_date: date
    quantity: int
    gross_pnl: Decimal
    net_pnl: Decimal
    total_commissions: Decimal
    entry_reason: str
    exit_reason: str
    detail_json: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(from_attributes=True)


class MultiStepRunDetailResponse(BaseModel):
    id: UUID
    name: str | None = None
    symbol: str
    workflow_type: str
    status: RunJobStatus
    start_date: date
    end_date: date
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    warnings: list[WarningResponse] = Field(default_factory=list)
    error_code: str | None = None
    error_message: str | None = None
    summary: BacktestSummaryResponse
    steps: list[MultiStepStepOutcomeResponse] = Field(default_factory=list)
    events: list[MultiStepEventResponse] = Field(default_factory=list)
    trades: list[MultiStepTradeResponse] = Field(default_factory=list)
    equity_curve: list[EquityCurvePointResponse] = Field(default_factory=list)


class MultiStepRunStatusResponse(BaseModel):
    id: UUID
    status: RunJobStatus
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None


class MultiStepRunHistoryItemResponse(BaseModel):
    id: UUID
    name: str | None = None
    symbol: str
    workflow_type: str
    status: RunJobStatus
    created_at: datetime
    completed_at: datetime | None = None
    summary: BacktestSummaryResponse


class MultiStepRunListResponse(CursorPaginatedResponse):
    items: list[MultiStepRunHistoryItemResponse]
