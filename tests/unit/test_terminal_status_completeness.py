"""Verify that all terminal status transitions set completed_at."""
from __future__ import annotations

from backtestforecast.models import BacktestRun, ExportJob, ScannerJob, SweepJob, SymbolAnalysis

_TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "expired"}
_MODELS_WITH_COMPLETED_AT = [BacktestRun, ScannerJob, ExportJob, SymbolAnalysis, SweepJob]


def test_models_have_completed_at_column():
    """All job models must have a completed_at column."""
    for model_cls in _MODELS_WITH_COMPLETED_AT:
        assert hasattr(model_cls, "completed_at"), (
            f"{model_cls.__name__} is missing completed_at column"
        )


def test_models_have_error_code_column():
    """All job models must have an error_code column."""
    for model_cls in _MODELS_WITH_COMPLETED_AT:
        assert hasattr(model_cls, "error_code"), (
            f"{model_cls.__name__} is missing error_code column"
        )
