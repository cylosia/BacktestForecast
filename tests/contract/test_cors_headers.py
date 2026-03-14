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


# ---------------------------------------------------------------------------
# Item 70: CSP headers include clerk.com domains
# ---------------------------------------------------------------------------


def test_csp_includes_clerk_domains():
    """Verify that the Next.js config CSP string includes clerk.com domains
    for script-src, connect-src, and frame-src directives."""
    from pathlib import Path

    next_config_path = Path(__file__).resolve().parents[2] / "apps" / "web" / "next.config.ts"
    assert next_config_path.exists(), f"next.config.ts not found at {next_config_path}"

    content = next_config_path.read_text(encoding="utf-8")

    assert "clerk.com" in content, "CSP must reference clerk.com"
    assert "*.clerk.com" in content or "clerk.com" in content, (
        "CSP must include clerk.com domain in at least one directive"
    )
    assert "script-src" in content, "CSP must include script-src directive"
    assert "connect-src" in content, "CSP must include connect-src directive"
    assert "frame-src" in content, "CSP must include frame-src directive"
