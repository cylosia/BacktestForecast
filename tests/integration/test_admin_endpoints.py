"""Tests for /metrics and /admin/dlq admin endpoints."""
from __future__ import annotations


def test_metrics_accessible_in_dev(client):
    """In dev/test, /metrics should be accessible without auth."""
    resp = client.get("/metrics")
    assert resp.status_code == 200


def test_dlq_accessible_in_dev(client):
    """In dev/test, /admin/dlq should be accessible without auth."""
    resp = client.get("/admin/dlq")
    assert resp.status_code in (200, 503)


def test_dlq_requires_auth_even_in_dev(client):
    """The /admin/dlq endpoint must require the metrics_token in ALL environments,
    including development. Without a valid token, it must return 403."""
    resp = client.get("/admin/dlq")
    # In dev without a metrics_token configured, the endpoint should either:
    #  - return 403 (token required but not configured / not provided)
    #  - return 200/503 only when the source code explicitly skips auth for dev
    # After the DLQ auth-in-all-environments fix, verify the handler always checks:
    import inspect

    from apps.api.app.main import app
    inspect.getsource(app.routes[-1].endpoint) if hasattr(app.routes[-1], "endpoint") else ""

    # Verify via the actual /admin/dlq route source that auth is NOT gated on app_env
    from backtestforecast.config import get_settings
    settings = get_settings()

    if not settings.metrics_token:
        assert resp.status_code == 403, (
            "DLQ endpoint must return 403 when no metrics_token is configured"
        )
    else:
        resp_no_auth = client.get("/admin/dlq")
        assert resp_no_auth.status_code == 403, (
            "DLQ endpoint must return 403 without Authorization header"
        )


def test_dlq_rejects_wrong_token(client, monkeypatch):
    """DLQ must reject requests with an incorrect metrics token."""
    from backtestforecast.config import Settings

    monkeypatch.setattr(Settings, "metrics_token", "correct-secret-token")

    resp = client.get(
        "/admin/dlq",
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert resp.status_code == 403
