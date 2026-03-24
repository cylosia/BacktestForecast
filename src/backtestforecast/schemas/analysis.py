from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from backtestforecast.schemas.backtests import (
    SYMBOL_ALLOWED_CHARS,
    BacktestSummaryResponse,
    EquityCurvePointResponse,
    TradeJsonResponse,
)
from backtestforecast.schemas.common import CursorPaginatedResponse, RunJobStatus, sanitize_error_message

DailyPicksStatus = Literal["ok", "no_data"]


class AnalysisSummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    symbol: str
    status: RunJobStatus
    stage: Literal["pending", "regime", "landscape", "deep_dive", "forecast"]
    close_price: Decimal | None = None
    strategies_tested: int
    configs_tested: int
    top_results_count: int
    duration_seconds: Decimal | None = None
    error_message: str | None = None
    error_code: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None

    _sanitize = field_validator("error_message", mode="before")(sanitize_error_message)


class RegimeDetail(BaseModel):
    model_config = ConfigDict(extra="ignore")
    regimes: list[str] = Field(default_factory=list)
    rsi_14: float | None = None
    ema_8: float | None = None
    ema_21: float | None = None
    sma_50: float | None = None
    sma_200: float | None = None
    realized_vol_20: float | None = None
    iv_rank_proxy: float | None = None
    volume_ratio: float | None = None
    close_price: float | None = None


class LandscapeCell(BaseModel):
    """Metrics use float (not Decimal) because they are read from JSONB storage.
    PostgreSQL JSONB does not preserve Decimal; values arrive as floats.
    BacktestSummaryResponse uses Decimal for API responses; this schema reflects DB shape."""
    model_config = {"extra": "ignore"}
    strategy_type: str
    strategy_label: str = ""
    target_dte: int = 0
    config: dict[str, Any] = Field(default_factory=dict)
    trade_count: int = 0
    win_rate: float = 0.0
    total_roi_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    score: float = 0.0


class AnalysisTopResult(BaseModel):
    model_config = ConfigDict(extra="ignore")
    rank: int = 0
    strategy_type: str = ""
    strategy_label: str = ""
    target_dte: int = 0
    config: dict[str, Any] | None = None
    summary: BacktestSummaryResponse | None = None
    trades: list[TradeJsonResponse] = Field(default_factory=list, max_length=10000)
    equity_curve: list[EquityCurvePointResponse] = Field(default_factory=list, max_length=10000)
    forecast: dict[str, Any] | None = None
    score: float = 0.0


class AnalysisDetailResponse(AnalysisSummaryResponse):
    model_config = {"from_attributes": True, "populate_by_name": True}

    regime: RegimeDetail | None = Field(default=None, validation_alias="regime_json")
    landscape: list[LandscapeCell] | None = Field(default=None, validation_alias="landscape_json")
    top_results: list[AnalysisTopResult] | None = Field(default=None, validation_alias="top_results_json")
    forecast: dict[str, Any] | None = Field(default=None, validation_alias="forecast_json")
    integrity_warnings: list[str] = Field(default_factory=list)


class CreateAnalysisRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=16)
    idempotency_key: str | None = Field(
        default=None,
        min_length=4,
        max_length=80,
        description="Optional client-generated key for retry-safe analysis creation. Reusing the key returns the existing analysis job rather than creating a duplicate.",
    )

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        v = v.strip().upper()
        if not SYMBOL_ALLOWED_CHARS.match(v):
            raise ValueError(
                "Symbol must be 1-16 characters starting with a letter (A-Z, 0-9, ., /, ^)."
            )
        return v


class AnalysisListResponse(CursorPaginatedResponse):
    items: list[AnalysisSummaryResponse]


# ---------------------------------------------------------------------------
# Daily Picks
# ---------------------------------------------------------------------------


class PipelineStatsResponse(BaseModel):
    symbols_screened: int
    symbols_after_screen: int
    pairs_generated: int
    quick_backtests_run: int
    full_backtests_run: int
    recommendations_produced: int
    duration_seconds: Decimal | None = None
    completed_at: datetime | None = None


class DailyPickSummary(BaseModel):
    """Known keys produced by the backtest engine for daily pick summaries."""
    model_config = ConfigDict(extra="ignore")
    trade_count: int = 0
    win_rate: float = 0.0
    total_roi_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    total_net_pnl: float = 0.0
    profit_factor: float | None = None
    sharpe_ratio: float | None = None
    expectancy: float = 0.0


class DailyPickForecast(BaseModel):
    """Known keys produced by the forecaster for daily pick forecasts."""
    model_config = ConfigDict(extra="ignore")
    expected_return_median_pct: float | None = None
    positive_outcome_rate_pct: float | None = None
    analog_count: int | None = None


class DailyPickItemResponse(BaseModel):
    rank: int
    score: Decimal
    symbol: str
    strategy_type: str
    regime_labels: list[str] = Field(default_factory=list)
    close_price: Decimal
    target_dte: int
    config_snapshot: dict[str, Any] | None = None
    summary: DailyPickSummary | None = None
    forecast: DailyPickForecast | None = None

    @field_validator("regime_labels", mode="before")
    @classmethod
    def coerce_regime_labels(cls, v: Any) -> list[str]:
        if not isinstance(v, list):
            return []
        return [str(item) for item in v if item is not None]


class DailyPicksResponse(BaseModel):
    trade_date: date | None = None
    pipeline_run_id: UUID | None = None
    status: DailyPicksStatus
    items: list[DailyPickItemResponse] = Field(default_factory=list)
    pipeline_stats: PipelineStatsResponse | None = None


class PipelineHistoryItemResponse(BaseModel):
    id: UUID
    trade_date: date
    status: RunJobStatus
    symbols_screened: int
    recommendations_produced: int
    duration_seconds: Decimal | None = None
    completed_at: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None

    _sanitize = field_validator("error_message", mode="before")(sanitize_error_message)


class PipelineHistoryResponse(CursorPaginatedResponse):
    items: list[PipelineHistoryItemResponse]
