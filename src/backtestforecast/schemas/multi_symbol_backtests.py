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
    EquityCurvePointResponse,
    StrategyOverrides,
    StrategyType,
)
from backtestforecast.schemas.common import CursorPaginatedResponse, RunJobStatus, WarningResponse


class MultiSymbolPriceRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    left_symbol: str = Field(min_length=1, max_length=16)
    left_indicator: str = Field(min_length=1, max_length=64)
    operator: Literal["lt", "lte", "gt", "gte", "eq", "neq"]
    right_symbol: str | None = Field(default=None, min_length=1, max_length=16)
    right_indicator: str | None = Field(default=None, min_length=1, max_length=64)
    threshold: Decimal | None = None
    lookback_period: int | None = Field(default=None, ge=1, le=252)

    @field_validator("left_symbol", "right_symbol")
    @classmethod
    def normalize_symbol(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().upper()
        if not SYMBOL_ALLOWED_CHARS.match(normalized):
            raise ValueError("Invalid symbol")
        return normalized

    @model_validator(mode="after")
    def validate_operand(self) -> MultiSymbolPriceRule:
        if self.right_symbol is None and self.threshold is None:
            raise ValueError("Either right_symbol/right_indicator or threshold is required")
        if self.right_symbol is not None and self.right_indicator is None:
            raise ValueError("right_indicator is required when right_symbol is provided")
        if self.right_symbol is None and self.right_indicator is not None:
            raise ValueError("right_indicator may only be set with right_symbol")
        return self


CrossSymbolRule = MultiSymbolPriceRule


class MultiSymbolDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=16)
    risk_per_trade_pct: Decimal = Field(gt=0, le=100)
    max_open_positions: int = Field(default=1, ge=1, le=10)
    capital_allocation_pct: Decimal | None = Field(default=None, gt=0, le=100)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not SYMBOL_ALLOWED_CHARS.match(normalized):
            raise ValueError("Invalid symbol")
        return normalized


class MultiSymbolLegDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=16)
    strategy_type: StrategyType
    target_dte: int = Field(ge=1, le=365)
    dte_tolerance_days: int = Field(default=5, ge=0, le=60)
    max_holding_days: int = Field(ge=1, le=120)
    quantity_mode: Literal["risk_based", "fixed_contracts"]
    fixed_contracts: int | None = Field(default=None, ge=1, le=100)
    custom_legs: list[CustomLegDefinition] | None = Field(default=None, max_length=8)
    strategy_overrides: StrategyOverrides | None = None

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not SYMBOL_ALLOWED_CHARS.match(normalized):
            raise ValueError("Invalid symbol")
        return normalized

    @model_validator(mode="after")
    def validate_quantity_mode(self) -> MultiSymbolLegDefinition:
        if self.quantity_mode == "fixed_contracts" and self.fixed_contracts is None:
            raise ValueError("fixed_contracts is required when quantity_mode is fixed_contracts")
        return self


class MultiSymbolStrategyGroup(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=120)
    synchronous_entry: bool = True
    legs: list[MultiSymbolLegDefinition] = Field(min_length=1, max_length=8)

    @model_validator(mode="after")
    def validate_group(self) -> MultiSymbolStrategyGroup:
        if self.synchronous_entry is not True:
            raise ValueError("Only synchronous_entry=true is currently supported")
        return self


class CreateMultiSymbolRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, max_length=120)
    symbols: list[MultiSymbolDefinition] = Field(min_length=2, max_length=3)
    strategy_groups: list[MultiSymbolStrategyGroup] = Field(min_length=1, max_length=6)
    entry_rules: list[CrossSymbolRule] = Field(min_length=1, max_length=12)
    exit_rules: list[CrossSymbolRule] = Field(default_factory=list, max_length=12)
    start_date: date
    end_date: date
    account_size: Decimal = Field(ge=Decimal("100"), le=Decimal("100000000"))
    capital_allocation_mode: Literal["equal_weight", "explicit"] = "equal_weight"
    commission_per_contract: Decimal = Field(ge=0, le=Decimal("100"))
    slippage_pct: Decimal = Field(default=Decimal("0"), ge=Decimal("0"), le=Decimal("5"))
    idempotency_key: str | None = Field(default=None, min_length=4, max_length=80)

    @model_validator(mode="after")
    def validate_request(self) -> CreateMultiSymbolRunRequest:
        from backtestforecast.utils.dates import market_date_today

        if self.end_date > market_date_today():
            raise ValueError("end_date cannot be in the future (US Eastern time).")
        if self.start_date >= self.end_date:
            raise ValueError("start_date must be earlier than end_date")

        symbol_set = {item.symbol for item in self.symbols}
        if len(symbol_set) != len(self.symbols):
            raise ValueError("symbols must be unique")

        for group in self.strategy_groups:
            for leg in group.legs:
                if leg.symbol not in symbol_set:
                    raise ValueError(f"Strategy leg symbol '{leg.symbol}' must be declared in symbols")

        if self.capital_allocation_mode == "explicit":
            total = sum(item.capital_allocation_pct or Decimal("0") for item in self.symbols)
            if total != Decimal("100"):
                raise ValueError("Explicit capital_allocation_pct values must sum to 100")
        return self


class MultiSymbolRunSymbolSummaryResponse(BaseModel):
    symbol: str
    summary: BacktestSummaryResponse


class MultiSymbolTradeResponse(BaseModel):
    id: UUID
    trade_group_id: UUID
    symbol: str
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


class MultiSymbolTradeGroupResponse(BaseModel):
    id: UUID
    entry_date: date
    exit_date: date | None = None
    status: Literal["open", "closed", "cancelled"]
    trades: list[MultiSymbolTradeResponse] = Field(default_factory=list)


class MultiSymbolRunDetailResponse(BaseModel):
    id: UUID
    name: str | None = None
    status: RunJobStatus
    start_date: date
    end_date: date
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    warnings: list[WarningResponse] = Field(default_factory=list)
    error_code: str | None = None
    error_message: str | None = None
    symbols: list[MultiSymbolDefinition] = Field(default_factory=list)
    summary: BacktestSummaryResponse
    symbol_summaries: list[MultiSymbolRunSymbolSummaryResponse] = Field(default_factory=list)
    trade_groups: list[MultiSymbolTradeGroupResponse] = Field(default_factory=list)
    equity_curve: list[EquityCurvePointResponse] = Field(default_factory=list)
    symbol_equity_curves: dict[str, list[EquityCurvePointResponse]] = Field(default_factory=dict)


class MultiSymbolRunStatusResponse(BaseModel):
    id: UUID
    status: RunJobStatus
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None


class MultiSymbolRunHistoryItemResponse(BaseModel):
    id: UUID
    name: str | None = None
    status: RunJobStatus
    created_at: datetime
    completed_at: datetime | None = None
    symbols: list[str] = Field(default_factory=list)
    summary: BacktestSummaryResponse


class MultiSymbolRunListResponse(CursorPaginatedResponse):
    items: list[MultiSymbolRunHistoryItemResponse]
