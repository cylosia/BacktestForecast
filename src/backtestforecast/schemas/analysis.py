from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class AnalysisSummaryResponse(BaseModel):
    model_config = {"from_attributes": True}

    id: UUID
    symbol: str
    status: str
    stage: str
    close_price: float | None = None
    strategies_tested: int
    configs_tested: int
    top_results_count: int
    duration_seconds: float | None = None
    error_message: str | None = None
    created_at: datetime | None = None
    completed_at: datetime | None = None


class RegimeDetail(BaseModel):
    model_config = {"extra": "allow"}
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
    model_config = {"extra": "allow"}
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
    model_config = {"extra": "allow"}
    rank: int = 0
    strategy_type: str = ""
    strategy_label: str = ""
    target_dte: int = 0
    config: dict[str, Any] = Field(default_factory=dict)
    summary: dict[str, Any] = Field(default_factory=dict)
    trades: list[dict[str, Any]] = Field(default_factory=list)
    equity_curve: list[dict[str, Any]] = Field(default_factory=list)
    forecast: dict[str, Any] = Field(default_factory=dict)
    score: float = 0.0


class AnalysisDetailResponse(AnalysisSummaryResponse):
    regime: RegimeDetail | None = None
    landscape: list[LandscapeCell] | None = None
    top_results: list[AnalysisTopResult] | None = None
    forecast: dict[str, Any] | None = None


class CreateAnalysisRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=16)
    idempotency_key: str | None = Field(default=None, max_length=80)


class AnalysisListResponse(BaseModel):
    items: list[AnalysisSummaryResponse]
    total: int = 0
    offset: int = 0
    limit: int = 50


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
    duration_seconds: float | None = None
    completed_at: str | None = None


class DailyPickSummary(BaseModel):
    """Known keys produced by the backtest engine for daily pick summaries."""
    model_config = {"extra": "allow"}
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
    model_config = {"extra": "allow"}
    expected_return_median_pct: float | None = None
    positive_outcome_rate_pct: float | None = None
    analog_count: int | None = None


class DailyPickItemResponse(BaseModel):
    rank: int
    score: float
    symbol: str
    strategy_type: str
    regime_labels: list[str] = Field(default_factory=list)
    close_price: float
    target_dte: int
    config_snapshot: dict[str, Any] = Field(default_factory=dict)
    summary: DailyPickSummary = Field(default_factory=DailyPickSummary)
    forecast: DailyPickForecast = Field(default_factory=DailyPickForecast)


class DailyPicksResponse(BaseModel):
    trade_date: str | None = None
    pipeline_run_id: str | None = None
    status: str
    items: list[DailyPickItemResponse] = Field(default_factory=list)
    pipeline_stats: PipelineStatsResponse | None = None


class PipelineHistoryItemResponse(BaseModel):
    id: str
    trade_date: str
    status: str
    symbols_screened: int
    recommendations_produced: int
    duration_seconds: float | None = None
    completed_at: str | None = None
    error_message: str | None = None


class PipelineHistoryResponse(BaseModel):
    items: list[PipelineHistoryItemResponse]
    next_cursor: str | None = None
