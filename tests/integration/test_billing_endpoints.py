"""Tests for billing checkout and portal session endpoints."""
from __future__ import annotations

from tests.integration.test_api_critical_flows import _set_user_plan


def test_checkout_session_requires_auth(client):
    resp = client.post("/v1/billing/checkout-session", json={"tier": "pro", "billing_interval": "monthly"})
    assert resp.status_code in (401, 403)


def test_checkout_session_free_tier_rejected(client, auth_headers, db_session):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="free", subscription_status=None)
    resp = client.post(
        "/v1/billing/checkout-session",
        headers=auth_headers,
        json={"tier": "free", "billing_interval": "monthly"},
    )
    assert resp.status_code == 422 or resp.status_code == 400


def test_checkout_session_missing_stripe_config(client, auth_headers, db_session):
    """When Stripe is not configured, checkout should fail gracefully."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="free", subscription_status=None)
    resp = client.post(
        "/v1/billing/checkout-session",
        headers=auth_headers,
        json={"tier": "pro", "billing_interval": "monthly"},
    )
    assert resp.status_code >= 400


def test_portal_session_requires_auth(client):
    resp = client.post("/v1/billing/portal-session", json={"return_path": "/app/settings/billing"})
    assert resp.status_code in (401, 403)


def test_portal_session_billing_disabled(client, auth_headers, monkeypatch):
    """When feature_billing_enabled is False, portal-session returns 403/feature_locked."""
    from backtestforecast.config import Settings

    monkeypatch.setattr(Settings, "feature_billing_enabled", False)

    client.get("/v1/me", headers=auth_headers)
    resp = client.post(
        "/v1/billing/portal-session",
        headers=auth_headers,
        json={"return_path": "/app/settings/billing"},
    )
    assert resp.status_code == 403
    body = resp.json()
    assert body["error"]["code"] == "feature_locked"
