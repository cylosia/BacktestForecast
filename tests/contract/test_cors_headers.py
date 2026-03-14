"""Contract test: verify CORS allows X-Requested-With header."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from apps.api.app.main import app


@pytest.fixture
def cors_client():
    with TestClient(app, base_url="http://localhost") as client:
        yield client


def test_cors_allows_x_requested_with(cors_client):
    """Preflight OPTIONS request should confirm X-Requested-With is an allowed header."""
    resp = cors_client.options(
        "/v1/backtests",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "X-Requested-With",
        },
    )
    allowed = resp.headers.get("access-control-allow-headers", "")
    assert "x-requested-with" in allowed.lower(), (
        f"X-Requested-With must be in Access-Control-Allow-Headers, got: {allowed}"
    )
