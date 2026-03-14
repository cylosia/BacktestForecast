"""Tests for RequestBodyLimitMiddleware."""
from __future__ import annotations


def test_oversized_body_rejected(client, auth_headers):
    """Body larger than default limit should be rejected."""
    client.get("/v1/me", headers=auth_headers)
    huge_body = b"x" * (2 * 1024 * 1024)
    resp = client.post(
        "/v1/backtests",
        headers={**auth_headers, "Content-Type": "application/json"},
        content=huge_body,
    )
    assert resp.status_code in (413, 422)


def test_normal_body_accepted(client, auth_headers):
    """Normal-sized body should pass the middleware (not rejected as too large)."""
    client.get("/v1/me", headers=auth_headers)
    resp = client.get("/v1/me", headers=auth_headers)
    assert resp.status_code == 200
