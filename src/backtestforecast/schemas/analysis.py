from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class AnalysisSummaryResponse(BaseModel):
    id: str
    symbol: str
    status: str
    stage: str
    close_price: float | None = None
    strategies_tested: int
    configs_tested: int
    top_results_count: int
    duration_seconds: float | None = None
    error_message: str | None = None
    created_at: str | None = None
    completed_at: str | None = None


class AnalysisDetailResponse(AnalysisSummaryResponse):
    regime: dict[str, Any] | None = None
    landscape: list[dict[str, Any]] | None = None
    top_results: list[dict[str, Any]] | None = None
    forecast: dict[str, Any] | None = None


class AnalysisListResponse(BaseModel):
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
    duration_seconds: float | None = None
    completed_at: str | None = None


class DailyPickItemResponse(BaseModel):
    rank: int
    score: float
    symbol: str
    strategy_type: str
    regime_labels: list[str] = Field(default_factory=list)
    close_price: float
    target_dte: int
    config_snapshot: dict[str, Any] = Field(default_factory=dict)
    summary: dict[str, Any] = Field(default_factory=dict)
    forecast: dict[str, Any] = Field(default_factory=dict)


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
