"""Tests for CORS and Host header validation middleware."""
from __future__ import annotations

import pytest


def test_cors_rejects_unknown_origin(client, auth_headers):
    """Request with an unknown Origin should not receive CORS allow headers."""
    resp = client.get(
        "/v1/me",
        headers={**auth_headers, "Origin": "http://evil.com"},
    )
    assert "access-control-allow-origin" not in resp.headers or resp.headers[
        "access-control-allow-origin"
    ] != "http://evil.com"


def test_cors_allows_configured_origin(client, auth_headers):
    """Request with the configured localhost origin should receive CORS headers."""
    resp = client.get(
        "/v1/me",
        headers={**auth_headers, "Origin": "http://localhost:3000"},
    )
    assert resp.headers.get("access-control-allow-origin") == "http://localhost:3000"


def test_cors_preflight_returns_allowed_methods(client):
    """OPTIONS preflight should list allowed methods for a configured origin."""
    resp = client.options(
        "/v1/me",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert resp.status_code == 200
    allowed = resp.headers.get("access-control-allow-methods", "")
    assert "GET" in allowed


@pytest.mark.parametrize("bad_host", ["evil.com", "attacker.io:8000"])
def test_host_header_rejection(bad_host):
    """Requests with an untrusted Host header should be rejected by TrustedHostMiddleware."""
    from fastapi.testclient import TestClient
    from apps.api.app.main import app

    with TestClient(app, base_url=f"http://{bad_host}") as tc:
        resp = tc.get("/health/live")
        assert resp.status_code == 400
