"""Verify all authenticated endpoints reject unauthenticated requests.

Every protected endpoint must return 401 (or 403) when no Bearer token
is provided. This test parametrizes over all known protected routes.
"""
from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from apps.api.app.main import app


@pytest.fixture()
def anon_client() -> TestClient:
    """Unauthenticated client - no auth overrides applied."""
    app.dependency_overrides.clear()
    with TestClient(app, base_url="http://localhost", raise_server_exceptions=False) as tc:
        yield tc


_FAKE_UUID = str(uuid.uuid4())

PROTECTED_ENDPOINTS: list[tuple[str, str]] = [
    ("GET", "/v1/me"),
    ("DELETE", "/v1/account/me"),
    ("GET", "/v1/account/me/export"),
    ("GET", "/v1/backtests"),
    ("POST", "/v1/backtests"),
    ("GET", f"/v1/backtests/{_FAKE_UUID}"),
    ("GET", f"/v1/backtests/{_FAKE_UUID}/status"),
    ("DELETE", f"/v1/backtests/{_FAKE_UUID}"),
    ("POST", "/v1/backtests/compare"),
    ("GET", "/v1/scans"),
    ("POST", "/v1/scans"),
    ("GET", f"/v1/scans/{_FAKE_UUID}"),
    ("GET", f"/v1/scans/{_FAKE_UUID}/status"),
    ("GET", f"/v1/scans/{_FAKE_UUID}/recommendations"),
    ("DELETE", f"/v1/scans/{_FAKE_UUID}"),
    ("GET", "/v1/exports"),
    ("POST", "/v1/exports"),
    ("GET", f"/v1/exports/{_FAKE_UUID}"),
    ("GET", f"/v1/exports/{_FAKE_UUID}/status"),
    ("DELETE", f"/v1/exports/{_FAKE_UUID}"),
    ("GET", "/v1/templates"),
    ("POST", "/v1/templates"),
    ("GET", f"/v1/templates/{_FAKE_UUID}"),
    ("PATCH", f"/v1/templates/{_FAKE_UUID}"),
    ("DELETE", f"/v1/templates/{_FAKE_UUID}"),
    ("GET", "/v1/forecasts/AAPL"),
    ("GET", "/v1/strategy-catalog"),
    ("GET", "/v1/daily-picks"),
    ("GET", "/v1/daily-picks/history"),
    ("POST", "/v1/analysis"),
    ("GET", f"/v1/analysis/{_FAKE_UUID}"),
    ("GET", f"/v1/analysis/{_FAKE_UUID}/status"),
    ("DELETE", f"/v1/analysis/{_FAKE_UUID}"),
    ("GET", "/v1/analysis"),
    ("GET", "/v1/sweeps"),
    ("POST", "/v1/sweeps"),
    ("GET", f"/v1/sweeps/{_FAKE_UUID}"),
    ("GET", f"/v1/sweeps/{_FAKE_UUID}/status"),
    ("GET", f"/v1/sweeps/{_FAKE_UUID}/results"),
    ("DELETE", f"/v1/sweeps/{_FAKE_UUID}"),
    ("POST", "/v1/billing/checkout-session"),
    ("POST", "/v1/billing/portal-session"),
    ("GET", f"/v1/events/backtests/{_FAKE_UUID}"),
    ("GET", f"/v1/events/scans/{_FAKE_UUID}"),
    ("GET", f"/v1/events/exports/{_FAKE_UUID}"),
    ("GET", f"/v1/events/sweeps/{_FAKE_UUID}"),
    ("GET", f"/v1/events/analyses/{_FAKE_UUID}"),
]


@pytest.mark.parametrize(
    "method,path",
    PROTECTED_ENDPOINTS,
    ids=[f"{m} {p.split('/')[-1][:20]}" for m, p in PROTECTED_ENDPOINTS],
)
def test_endpoint_rejects_unauthenticated(
    anon_client: TestClient, method: str, path: str,
) -> None:
    """Every protected endpoint must return 401 without a valid Bearer token."""
    body: dict | bytes | None = None
    if method == "POST" or method == "PATCH":
        body = {}

    if method == "GET":
        resp = anon_client.get(path)
    elif method == "POST":
        resp = anon_client.post(path, json=body)
    elif method == "PATCH":
        resp = anon_client.patch(path, json=body)
    elif method == "DELETE":
        resp = anon_client.delete(path)
    else:
        pytest.fail(f"Unsupported method: {method}")

    # 422 is NOT acceptable - it means the request bypassed auth and reached
    # input validation.  Only 401 (unauthenticated) or 403 (forbidden) prove
    # the auth layer rejected the request before any business logic ran.
    assert resp.status_code in (401, 403), (
        f"{method} {path} returned {resp.status_code} instead of 401/403. "
        f"Body: {resp.text[:200]}"
    )


class TestUnauthenticatedPublicEndpoints:
    """Verify that truly public endpoints remain accessible."""

    def test_health_live_is_public(self, anon_client: TestClient) -> None:
        resp = anon_client.get("/health/live")
        assert resp.status_code == 200

    def test_health_ready_is_public(self, anon_client: TestClient) -> None:
        resp = anon_client.get("/health/ready")
        assert resp.status_code in (200, 499, 503)

    def test_stripe_webhook_accepts_without_bearer(self, anon_client: TestClient) -> None:
        """Stripe webhook uses signature verification, not Bearer auth."""
        resp = anon_client.post(
            "/v1/billing/webhook",
            content=b"{}",
            headers={"Stripe-Signature": "t=0,v1=test", "Host": "localhost"},
        )
        # The webhook is public from an auth perspective. It may still fail due to
        # signature validation, body validation, or fail-closed rate limiting.
        assert resp.status_code in (200, 401, 422, 503)
