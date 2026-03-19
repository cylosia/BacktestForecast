"""Fix 79: All job models must have a last_heartbeat_at column.

Schema-level test that verifies BacktestRun, ScannerJob, SweepJob,
ExportJob, SymbolAnalysis, and NightlyPipelineRun all define the
last_heartbeat_at column.
"""
from __future__ import annotations

import pytest

from backtestforecast.models import (
    BacktestRun,
    ExportJob,
    NightlyPipelineRun,
    ScannerJob,
    SweepJob,
    SymbolAnalysis,
)

_JOB_MODELS = [
    BacktestRun,
    ScannerJob,
    SweepJob,
    ExportJob,
    SymbolAnalysis,
    NightlyPipelineRun,
]


class TestHeartbeatColumn:
    """Verify last_heartbeat_at exists on all job models."""

    @pytest.mark.parametrize(
        "model",
        _JOB_MODELS,
        ids=[m.__name__ for m in _JOB_MODELS],
    )
    def test_model_has_last_heartbeat_at_attribute(self, model):
        """Each job model must have a last_heartbeat_at mapped column."""
        assert hasattr(model, "last_heartbeat_at"), (
            f"{model.__name__} is missing the last_heartbeat_at column"
        )

    @pytest.mark.parametrize(
        "model",
        _JOB_MODELS,
        ids=[m.__name__ for m in _JOB_MODELS],
    )
    def test_last_heartbeat_at_in_table_columns(self, model):
        """last_heartbeat_at must appear in the SQL table definition."""
        table = model.__table__
        column_names = {c.name for c in table.columns}
        assert "last_heartbeat_at" in column_names, (
            f"{model.__name__}.__table__ is missing 'last_heartbeat_at' column"
        )

    @pytest.mark.parametrize(
        "model",
        _JOB_MODELS,
        ids=[m.__name__ for m in _JOB_MODELS],
    )
    def test_last_heartbeat_at_is_nullable(self, model):
        """last_heartbeat_at should be nullable (jobs start without a heartbeat)."""
        col = model.__table__.c.last_heartbeat_at
        assert col.nullable, (
            f"{model.__name__}.last_heartbeat_at must be nullable"
        )
