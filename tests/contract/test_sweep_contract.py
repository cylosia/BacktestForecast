"""Contract test: verify sweep TypeScript types include all backend schema fields."""
from __future__ import annotations

from pathlib import Path

TS_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "packages" / "api-client" / "src" / "schema.d.ts"


def test_sweep_job_response_fields_in_typescript():
    """Every SweepJobResponse field must appear in the TypeScript type definition."""
    from backtestforecast.schemas.sweeps import SweepJobResponse

    assert TS_SCHEMA_PATH.exists(), f"TypeScript file not found: {TS_SCHEMA_PATH}"
    ts_content = TS_SCHEMA_PATH.read_text(encoding="utf-8")

    for field_name, field_info in SweepJobResponse.model_fields.items():
        ts_name = field_info.serialization_alias or field_info.validation_alias or field_info.alias or field_name
        assert ts_name in ts_content or field_name in ts_content, (
            f"SweepJobResponse.{field_name} (serialized as '{ts_name}') "
            f"is missing from TypeScript definitions"
        )


def test_sweep_result_response_fields_in_typescript():
    """Every SweepResultResponse field must appear in the TypeScript type definition."""
    from backtestforecast.schemas.sweeps import SweepResultResponse

    assert TS_SCHEMA_PATH.exists(), f"TypeScript file not found: {TS_SCHEMA_PATH}"
    ts_content = TS_SCHEMA_PATH.read_text(encoding="utf-8")

    for field_name, field_info in SweepResultResponse.model_fields.items():
        ts_name = field_info.serialization_alias or field_info.validation_alias or field_info.alias or field_name
        assert ts_name in ts_content or field_name in ts_content, (
            f"SweepResultResponse.{field_name} (serialized as '{ts_name}') "
            f"is missing from TypeScript definitions"
        )
