"""Contract test: verify Pydantic response schemas match the OpenAPI snapshot.

Catches drift between backend schema changes and the generated OpenAPI spec
that the frontend TypeScript client is built from.  If a field is added to
a Pydantic response model but the OpenAPI snapshot is not regenerated, the
frontend will be missing the new field.

Run after any Pydantic schema change:
    python -m pytest tests/contract/test_schema_field_parity.py -v
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import AliasChoices

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_SNAPSHOT = PROJECT_ROOT / "openapi.snapshot.json"


@pytest.fixture(scope="module")
def openapi_schemas() -> dict[str, dict]:
    """Load the component schemas from the OpenAPI snapshot."""
    if not OPENAPI_SNAPSHOT.exists():
        pytest.skip("openapi.snapshot.json not found - run scripts/export_openapi.py first")
    data = json.loads(OPENAPI_SNAPSHOT.read_text(encoding="utf-8"))
    return data.get("components", {}).get("schemas", {})


def _missing_openapi_fields(model_cls, openapi_fields: set[str]) -> set[str]:
    """Return canonical field names with no acceptable OpenAPI representation."""

    missing: set[str] = set()
    for name, info in model_cls.model_fields.items():
        accepted = {info.alias if info.alias else name}
        validation_alias = getattr(info, "validation_alias", None)
        if isinstance(validation_alias, str):
            accepted.add(validation_alias)
        elif isinstance(validation_alias, AliasChoices):
            accepted.update(choice for choice in validation_alias.choices if isinstance(choice, str))
        if not accepted & openapi_fields:
            missing.add(name)
    return missing


def _get_openapi_fields(schema: dict) -> set[str]:
    """Return the set of property names from an OpenAPI component schema."""
    return set(schema.get("properties", {}).keys())


class TestBacktestSchemas:
    def test_backtest_run_detail_response(self, openapi_schemas):
        from backtestforecast.schemas.backtests import BacktestRunDetailResponse
        openapi_fields = _get_openapi_fields(openapi_schemas.get("BacktestRunDetailResponse", {}))
        if not openapi_fields:
            pytest.skip("BacktestRunDetailResponse not in OpenAPI snapshot")
        missing_from_openapi = _missing_openapi_fields(BacktestRunDetailResponse, openapi_fields)
        assert not missing_from_openapi, (
            f"Fields in Pydantic but missing from OpenAPI snapshot: {missing_from_openapi}. "
            "Regenerate: python scripts/export_openapi.py > openapi.snapshot.json"
        )

    def test_backtest_summary_response(self, openapi_schemas):
        from backtestforecast.schemas.backtests import BacktestSummaryResponse
        openapi_fields = _get_openapi_fields(openapi_schemas.get("BacktestSummaryResponse", {}))
        if not openapi_fields:
            pytest.skip("BacktestSummaryResponse not in OpenAPI snapshot")
        missing = _missing_openapi_fields(BacktestSummaryResponse, openapi_fields)
        assert not missing, f"Missing from OpenAPI: {missing}"


class TestExportSchemas:
    def test_export_job_response(self, openapi_schemas):
        from backtestforecast.schemas.exports import ExportJobResponse
        openapi_fields = _get_openapi_fields(openapi_schemas.get("ExportJobResponse", {}))
        if not openapi_fields:
            pytest.skip("ExportJobResponse not in OpenAPI snapshot")
        missing = _missing_openapi_fields(ExportJobResponse, openapi_fields)
        assert not missing, f"Missing from OpenAPI: {missing}"


class TestScannerSchemas:
    def test_scanner_job_response(self, openapi_schemas):
        from backtestforecast.schemas.scans import ScannerJobResponse
        openapi_fields = _get_openapi_fields(openapi_schemas.get("ScannerJobResponse", {}))
        if not openapi_fields:
            pytest.skip("ScannerJobResponse not in OpenAPI snapshot")
        missing = _missing_openapi_fields(ScannerJobResponse, openapi_fields)
        assert not missing, f"Missing from OpenAPI: {missing}"


class TestBillingSchemas:
    def test_checkout_session_response(self, openapi_schemas):
        from backtestforecast.schemas.billing import CheckoutSessionResponse
        openapi_fields = _get_openapi_fields(openapi_schemas.get("CheckoutSessionResponse", {}))
        if not openapi_fields:
            pytest.skip("CheckoutSessionResponse not in OpenAPI snapshot")
        missing = _missing_openapi_fields(CheckoutSessionResponse, openapi_fields)
        assert not missing, f"Missing from OpenAPI: {missing}"


class TestSweepSchemas:
    """Sweep types are manually maintained in the frontend TypeScript.
    This test verifies the backend Pydantic model fields match what
    the manual TypeScript interface declares."""

    def test_sweep_job_response_fields_present_in_typescript(self):
        from backtestforecast.schemas.sweeps import SweepJobResponse

        ts_path = PROJECT_ROOT / "packages" / "api-client" / "src" / "schema.d.ts"
        if not ts_path.exists():
            pytest.skip("TypeScript api-client not found")

        ts_content = ts_path.read_text(encoding="utf-8")
        missing = []
        for field_name, info in SweepJobResponse.model_fields.items():
            ts_name = info.alias if info.alias else field_name
            if ts_name not in ts_content and field_name not in ts_content:
                missing.append(f"{field_name} (ts: {ts_name})")

        assert not missing, (
            f"SweepJobResponse fields missing from TypeScript: {missing}"
        )

    def test_sweep_result_response_fields_present_in_typescript(self):
        from backtestforecast.schemas.sweeps import SweepResultResponse

        ts_path = PROJECT_ROOT / "packages" / "api-client" / "src" / "schema.d.ts"
        if not ts_path.exists():
            pytest.skip("TypeScript api-client not found")

        ts_content = ts_path.read_text(encoding="utf-8")
        missing = []
        for field_name, info in SweepResultResponse.model_fields.items():
            ts_name = info.alias if info.alias else field_name
            if ts_name not in ts_content and field_name not in ts_content:
                missing.append(f"{field_name} (ts: {ts_name})")

        assert not missing, (
            f"SweepResultResponse fields missing from TypeScript: {missing}"
        )


class TestErrorEnvelope:
    def test_error_envelope_has_detail_field(self, openapi_schemas):
        """The ErrorEnvelope must include the 'detail' field for quota errors."""
        envelope = openapi_schemas.get("ErrorEnvelope", {})
        if not envelope:
            pytest.skip("ErrorEnvelope not in OpenAPI snapshot")
        error_props = envelope.get("properties", {}).get("error", {}).get("properties", {})
        assert "detail" in error_props, (
            "ErrorEnvelope.error must include 'detail' for quota/feature error context"
        )
        detail_props = error_props["detail"].get("properties", {})
        assert "current_tier" in detail_props
        assert "required_tier" in detail_props
