from __future__ import annotations

from backtestforecast.schemas.analysis import AnalysisListResponse
from backtestforecast.schemas.backtests import BacktestRunListResponse
from backtestforecast.schemas.common import CursorPaginatedResponse
from backtestforecast.schemas.exports import ExportJobListResponse
from backtestforecast.schemas.scans import ScannerJobListResponse
from backtestforecast.schemas.sweeps import SweepJobListResponse


def test_cursor_paginated_responses_expose_stable_total_contract() -> None:
    """Cursor-paginated list responses must expose total/offset/limit/next_cursor together."""
    expected = {"items", "total", "offset", "limit", "next_cursor"}
    for model in (
        BacktestRunListResponse,
        ExportJobListResponse,
        ScannerJobListResponse,
        SweepJobListResponse,
        AnalysisListResponse,
    ):
        assert expected.issubset(model.model_fields.keys()), model.__name__


def test_cursor_paginated_responses_share_common_base_model() -> None:
    for model in (
        BacktestRunListResponse,
        ExportJobListResponse,
        ScannerJobListResponse,
        SweepJobListResponse,
        AnalysisListResponse,
    ):
        assert issubclass(model, CursorPaginatedResponse), model.__name__
