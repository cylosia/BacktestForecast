"""Contract tests: validation_alias fields serialize by field name, not alias.

All response schemas that use validation_alias (to read from ORM column names
like ``warnings_json``) must serialize as the Pydantic field name (``warnings``),
not the alias. This prevents frontend-backend contract mismatches.
"""
from __future__ import annotations

import pytest

from backtestforecast.schemas.analysis import AnalysisDetailResponse
from backtestforecast.schemas.backtests import BacktestRunDetailResponse
from backtestforecast.schemas.scans import ScannerJobResponse
from backtestforecast.schemas.sweeps import SweepJobResponse
from backtestforecast.schemas.templates import TemplateResponse

_SCHEMAS_WITH_VALIDATION_ALIAS = [
    (BacktestRunDetailResponse, "warnings", "warnings_json"),
    (ScannerJobResponse, "warnings", "warnings_json"),
    (SweepJobResponse, "warnings", "warnings_json"),
    (SweepJobResponse, "prefetch_summary", "prefetch_summary_json"),
    (SweepJobResponse, "request_snapshot", "request_snapshot_json"),
    (TemplateResponse, "config", "config_json"),
    (AnalysisDetailResponse, "regime", "regime_json"),
    (AnalysisDetailResponse, "landscape", "landscape_json"),
    (AnalysisDetailResponse, "top_results", "top_results_json"),
    (AnalysisDetailResponse, "forecast", "forecast_json"),
]


@pytest.mark.parametrize(
    "schema_cls,field_name,expected_validation_alias",
    _SCHEMAS_WITH_VALIDATION_ALIAS,
    ids=[f"{c.__name__}.{f}" for c, f, _ in _SCHEMAS_WITH_VALIDATION_ALIAS],
)
def test_validation_alias_not_used_for_serialization(
    schema_cls, field_name: str, expected_validation_alias: str,
) -> None:
    """Field must use validation_alias (not alias) so serialization uses the field name."""
    field_info = schema_cls.model_fields[field_name]
    assert field_info.validation_alias == expected_validation_alias, (
        f"{schema_cls.__name__}.{field_name}: expected validation_alias={expected_validation_alias!r}, "
        f"got {field_info.validation_alias!r}"
    )
    assert field_info.alias is None, (
        f"{schema_cls.__name__}.{field_name}: alias must be None (not {field_info.alias!r}) "
        f"to prevent serialization as the alias name"
    )
