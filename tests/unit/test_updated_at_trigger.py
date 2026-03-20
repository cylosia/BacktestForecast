"""Verify that every model with an updated_at column is correctly mapped."""
from __future__ import annotations

from sqlalchemy import DateTime, inspect as sa_inspect

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


def test_updated_at_column_is_datetime_with_timezone():
    """updated_at columns must be DateTime(timezone=True)."""
    for model_cls in _MODELS_WITH_UPDATED_AT:
        mapper = sa_inspect(model_cls)
        col = mapper.columns.get("updated_at")
        assert col is not None, f"{model_cls.__name__} has no mapped updated_at column"
        assert isinstance(col.type, DateTime), (
            f"{model_cls.__name__}.updated_at should be DateTime, got {type(col.type).__name__}"
        )
        assert col.type.timezone is True, (
            f"{model_cls.__name__}.updated_at must have timezone=True"
        )


def test_updated_at_column_has_server_default():
    """updated_at columns must have a server_default (func.now())."""
    for model_cls in _MODELS_WITH_UPDATED_AT:
        mapper = sa_inspect(model_cls)
        col = mapper.columns.get("updated_at")
        assert col is not None, f"{model_cls.__name__} has no mapped updated_at column"
        assert col.server_default is not None, (
            f"{model_cls.__name__}.updated_at must have server_default"
        )
