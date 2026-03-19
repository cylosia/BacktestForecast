"""Contract test: verify sweep TypeScript types include all backend schema fields."""
from __future__ import annotations

from pathlib import Path


def test_sweep_job_response_fields_in_typescript():
    """Every SweepJobResponse field must appear in the TypeScript type definition."""
    from backtestforecast.schemas.sweeps import SweepJobResponse

    ts_path = Path(__file__).resolve().parents[2] / "packages" / "api-client" / "src" / "index.ts"
    assert ts_path.exists(), f"TypeScript file not found: {ts_path}"
    ts_content = ts_path.read_text(encoding="utf-8")

    for field_name, field_info in SweepJobResponse.model_fields.items():
        ts_name = field_info.alias if field_info.alias else field_name
        assert ts_name in ts_content or field_name in ts_content, (
            f"SweepJobResponse.{field_name} (serialized as '{ts_name}') "
            f"is missing from TypeScript definitions"
        )


def test_sweep_result_response_fields_in_typescript():
    """Every SweepResultResponse field must appear in the TypeScript type definition."""
    from backtestforecast.schemas.sweeps import SweepResultResponse

    ts_path = Path(__file__).resolve().parents[2] / "packages" / "api-client" / "src" / "index.ts"
    assert ts_path.exists(), f"TypeScript file not found: {ts_path}"
    ts_content = ts_path.read_text(encoding="utf-8")

    for field_name, field_info in SweepResultResponse.model_fields.items():
        ts_name = field_info.alias if field_info.alias else field_name
        assert ts_name in ts_content or field_name in ts_content, (
            f"SweepResultResponse.{field_name} (serialized as '{ts_name}') "
            f"is missing from TypeScript definitions"
        )
