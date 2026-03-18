"""Verify that every model with an updated_at column is correctly mapped."""
from __future__ import annotations

from backtestforecast.models import (
    BacktestRun,
    BacktestTemplate,
    DailyRecommendation,
    ExportJob,
    NightlyPipelineRun,
    OutboxMessage,
    ScannerJob,
    ScannerRecommendation,
    StripeEvent,
    SweepJob,
    SweepResult,
    SymbolAnalysis,
    User,
)

_MODELS_WITH_UPDATED_AT = [
    User,
    BacktestRun,
    ScannerRecommendation,
    BacktestTemplate,
    ScannerJob,
    ExportJob,
    NightlyPipelineRun,
    DailyRecommendation,
    StripeEvent,
    SymbolAnalysis,
    SweepJob,
    SweepResult,
    OutboxMessage,
]


def test_updated_at_columns_exist():
    """Every model that should have updated_at must expose the column."""
    for model_cls in _MODELS_WITH_UPDATED_AT:
        assert hasattr(model_cls, "updated_at"), (
            f"{model_cls.__name__} missing updated_at"
        )
