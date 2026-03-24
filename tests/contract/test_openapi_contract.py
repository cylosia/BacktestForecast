"""Contract tests: verify the runtime FastAPI app's OpenAPI schema matches key expectations."""

from __future__ import annotations

import pytest

from apps.api.app.main import app


@pytest.fixture
def openapi_schema() -> dict:
    """Return the runtime OpenAPI schema from the FastAPI app."""
    return app.openapi()


def test_security_scheme_defined(openapi_schema: dict) -> None:
    """Assert that components.securitySchemes.BearerAuth exists with type=http, scheme=bearer."""
    schemes = openapi_schema.get("components", {}).get("securitySchemes", {})
    assert "BearerAuth" in schemes, "BearerAuth security scheme must be defined"
    bearer = schemes["BearerAuth"]
    assert bearer.get("type") == "http", "BearerAuth must have type=http"
    assert bearer.get("scheme") == "bearer", "BearerAuth must have scheme=bearer"


def test_global_security_applied(openapi_schema: dict) -> None:
    """Assert that the top-level security list includes BearerAuth."""
    security = openapi_schema.get("security", [])
    assert security, "Top-level security must be defined"
    bearer_refs = [s for s in security if "BearerAuth" in s]
    assert bearer_refs, "Top-level security must include BearerAuth"


def test_422_uses_error_envelope(openapi_schema: dict) -> None:
    """For POST /v1/backtests, assert the 422 response references ErrorEnvelope not Pydantic ValidationError."""
    paths = openapi_schema.get("paths", {})
    post_backtests = paths.get("/v1/backtests", {}).get("post")
    assert post_backtests is not None, "POST /v1/backtests must exist"
    responses = post_backtests.get("responses", {})
    resp_422 = responses.get("422")
    assert resp_422 is not None, "422 response must be defined"
    content = resp_422.get("content", {})
    json_content = content.get("application/json", {})
    schema_ref = json_content.get("schema", {}).get("$ref")
    assert schema_ref is not None, "422 must have a schema reference"
    assert "ErrorEnvelope" in schema_ref, "422 must reference ErrorEnvelope, not default Pydantic ValidationError"


def test_create_backtest_entry_rules_boundary_is_documented(openapi_schema: dict) -> None:
    """The shared request schema may allow empty entry_rules internally, but the public create route must document the boundary."""
    create_schema = (
        openapi_schema.get("components", {})
        .get("schemas", {})
        .get("CreateBacktestRunRequest", {})
    )
    properties = create_schema.get("properties", {})
    entry_rules = properties.get("entry_rules", {})
    description = entry_rules.get("description", "")
    assert "empty list" in description.lower()
    assert "public create-backtest api rejects empty entry_rules" in description.lower()


def test_export_download_describes_binary(openapi_schema: dict) -> None:
    """Assert GET /v1/exports/{export_job_id} has responses with content types for text/csv, application/pdf,
    and application/octet-stream with format: binary."""
    paths = openapi_schema.get("paths", {})
    path_key = "/v1/exports/{export_job_id}"
    get_op = paths.get(path_key, {}).get("get")
    assert get_op is not None, f"GET {path_key} must exist"
    responses = get_op.get("responses", {})
    resp_200 = responses.get("200")
    assert resp_200 is not None, "200 response must be defined"
    content = resp_200.get("content", {})
    for mime in ("text/csv", "application/pdf", "application/octet-stream"):
        assert mime in content, f"200 must describe content type {mime}"
        schema = content[mime].get("schema", {})
        assert schema.get("format") == "binary", f"{mime} must have format: binary"


def test_sse_endpoints_describe_event_stream(openapi_schema: dict) -> None:
    """For each SSE endpoint, assert the 200 response has content type text/event-stream."""
    sse_paths = [
        "/v1/events/backtests/{run_id}",
        "/v1/events/scans/{job_id}",
        "/v1/events/exports/{export_job_id}",
        "/v1/events/analyses/{analysis_id}",
    ]
    paths = openapi_schema.get("paths", {})
    for path_key in sse_paths:
        get_op = paths.get(path_key, {}).get("get")
        assert get_op is not None, f"GET {path_key} must exist"
        responses = get_op.get("responses", {})
        resp_200 = responses.get("200")
        assert resp_200 is not None, f"200 response must be defined for {path_key}"
        content = resp_200.get("content", {})
        assert "text/event-stream" in content, (
            f"200 for {path_key} must have content type text/event-stream"
        )


def test_template_config_schema_exists(openapi_schema: dict) -> None:
    """Assert that the TemplateConfig-Output schema is present in the OpenAPI components."""
    schemas = openapi_schema.get("components", {}).get("schemas", {})
    matching = [name for name in schemas if name.startswith("TemplateConfig")]
    assert matching, (
        "TemplateConfig schema must exist in OpenAPI components (expected "
        "'TemplateConfig-Output' or similar). Found schemas: "
        + ", ".join(sorted(schemas.keys())[:20])
    )


def test_scanner_job_response_has_warnings_json_field() -> None:
    """Item 86: ScannerJobResponse must accept ``warnings_json`` from the ORM payload."""
    from backtestforecast.schemas.scans import ScannerJobResponse

    fields = ScannerJobResponse.model_fields
    assert "warnings" in fields, "ScannerJobResponse must have a 'warnings' field"
    validation_alias = fields["warnings"].validation_alias
    assert validation_alias == "warnings_json", (
        f"Expected validation alias 'warnings_json' for the warnings field, got '{validation_alias}'"
    )

    schema = ScannerJobResponse.model_json_schema(by_alias=True)
    assert "warnings_json" in schema.get("properties", {}), (
        "By-alias JSON schema must expose 'warnings_json' as the property name"
    )


# ---------------------------------------------------------------------------
# Item 45: scanner maxSymbols matches backend policy
# ---------------------------------------------------------------------------


def test_scanner_max_symbols_matches_backend_policy() -> None:
    """Verify the PRO basic max_symbols from ScannerAccessPolicy matches the
    frontend constant used in scanner-form.tsx (mode=basic -> 5)."""
    from backtestforecast.billing.entitlements import (
        POLICIES,
        PlanTier,
        ScannerAccessPolicy,
        ScannerMode,
    )

    pro_basic_policy = POLICIES[(PlanTier.PRO, ScannerMode.BASIC)]
    assert isinstance(pro_basic_policy, ScannerAccessPolicy)
    frontend_basic_max_symbols = 5
    assert pro_basic_policy.max_symbols == frontend_basic_max_symbols, (
        f"PRO basic max_symbols ({pro_basic_policy.max_symbols}) must match "
        f"frontend constant ({frontend_basic_max_symbols})"
    )


def test_error_envelope_schema_shape(openapi_schema: dict) -> None:
    """Assert the ErrorEnvelope schema has error.code and error.message as required fields."""
    schemas = openapi_schema.get("components", {}).get("schemas", {})
    assert "ErrorEnvelope" in schemas, "ErrorEnvelope schema must be defined"
    envelope = schemas["ErrorEnvelope"]
    assert "error" in envelope.get("required", []), "ErrorEnvelope must require 'error'"
    error_props = envelope.get("properties", {}).get("error", {})
    assert error_props, "ErrorEnvelope.error must be defined"
    error_required = error_props.get("required", [])
    assert "code" in error_required, "error.code must be required"
    assert "message" in error_required, "error.message must be required"


def test_cursor_paginated_totals_are_documented_as_pre_cursor_counts(openapi_schema: dict) -> None:
    """OpenAPI should explain that list totals are full matching counts, not post-cursor counts."""
    schemas = openapi_schema.get("components", {}).get("schemas", {})
    response_names = (
        "BacktestRunListResponse",
        "ExportJobListResponse",
        "ScannerJobListResponse",
        "SweepJobListResponse",
        "AnalysisListResponse",
    )
    for schema_name in response_names:
        total_schema = schemas.get(schema_name, {}).get("properties", {}).get("total", {})
        description = total_schema.get("description", "")
        assert "before page slicing" in description, schema_name
        assert "does not shrink after applying a cursor" in description, schema_name


# ---------------------------------------------------------------------------
# Item 74: forecast response includes trading_days_used
# ---------------------------------------------------------------------------


def test_forecast_response_includes_trading_days_used() -> None:
    """HistoricalAnalogForecastResponse schema must include trading_days_used and analogs_used."""
    from backtestforecast.schemas.scans import HistoricalAnalogForecastResponse

    fields = HistoricalAnalogForecastResponse.model_fields
    assert "trading_days_used" in fields
    assert "analogs_used" in fields


# ---------------------------------------------------------------------------
# Item 75: export cleanup response has size_bytes=0
# ---------------------------------------------------------------------------


def test_export_job_has_cleanup_fields() -> None:
    """ExportJob model must have size_bytes and sha256_hex for cleanup response."""
    from backtestforecast.models import ExportJob

    cols = {c.name for c in ExportJob.__table__.columns}
    assert "size_bytes" in cols
    assert "sha256_hex" in cols


# ---------------------------------------------------------------------------
# Key endpoints present in the schema
# ---------------------------------------------------------------------------


_EXPECTED_ENDPOINTS: list[tuple[str, str]] = [
    ("post", "/v1/backtests"),
    ("get", "/v1/backtests"),
    ("get", "/v1/backtests/{run_id}"),
    ("get", "/v1/backtests/{run_id}/status"),
    ("post", "/v1/backtests/compare"),
    ("post", "/v1/exports"),
    ("get", "/v1/exports/{export_job_id}"),
    ("get", "/v1/exports/{export_job_id}/status"),
    ("post", "/v1/scans"),
    ("get", "/v1/scans/{job_id}"),
    ("get", "/v1/scans/{job_id}/recommendations"),
    ("get", "/v1/forecasts/{ticker}"),
    ("post", "/v1/templates"),
    ("get", "/v1/templates"),
    ("get", "/v1/templates/{template_id}"),
    ("patch", "/v1/templates/{template_id}"),
    ("delete", "/v1/templates/{template_id}"),
    ("get", "/v1/me"),
    ("get", "/v1/strategy-catalog"),
    ("post", "/v1/billing/checkout-session"),
    ("post", "/v1/billing/webhook"),
    ("post", "/v1/analysis"),
    ("get", "/v1/analysis/{analysis_id}"),
    ("get", "/v1/daily-picks"),
]


@pytest.mark.parametrize("method,path", _EXPECTED_ENDPOINTS, ids=[f"{m.upper()} {p}" for m, p in _EXPECTED_ENDPOINTS])
def test_expected_endpoint_exists(openapi_schema: dict, method: str, path: str) -> None:
    """Verify that each key endpoint is present in the runtime OpenAPI spec."""
    paths = openapi_schema.get("paths", {})
    assert path in paths, f"Path {path} missing from OpenAPI schema. Available: {sorted(paths.keys())[:15]}"
    assert method in paths[path], (
        f"{method.upper()} {path} missing. Available methods: {list(paths[path].keys())}"
    )


_EXPECTED_SCHEMAS = [
    "BacktestRunDetailResponse",
    "BacktestSummaryResponse",
    "CreateBacktestRunRequest",
    "ExportJobResponse",
    "ScannerJobResponse",
    "TemplateResponse",
    "ErrorEnvelope",
]


@pytest.mark.parametrize("schema_name", _EXPECTED_SCHEMAS)
def test_expected_schema_exists(openapi_schema: dict, schema_name: str) -> None:
    """Verify that each key schema is present in the OpenAPI components."""
    schemas = openapi_schema.get("components", {}).get("schemas", {})
    matching = [name for name in schemas if name.startswith(schema_name.split("-")[0])]
    assert matching, (
        f"Schema {schema_name} missing from OpenAPI components. "
        f"Available (first 20): {sorted(schemas.keys())[:20]}"
    )
