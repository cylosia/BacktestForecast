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
