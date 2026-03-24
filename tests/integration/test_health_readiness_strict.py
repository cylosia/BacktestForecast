from __future__ import annotations

from unittest.mock import patch


def test_health_live_is_never_rate_limited(client):
    for _ in range(400):
        resp = client.get("/health/live")
        assert resp.status_code == 200


def test_health_ready_is_never_rate_limited(client):
    with patch("apps.api.app.routers.health.ping_database"):
        for _ in range(200):
            resp = client.get("/health/ready")
            assert resp.status_code in (200, 503)


def test_health_ready_returns_503_when_migration_drift_detected(client, monkeypatch):
    monkeypatch.setattr("apps.api.app.routers.health.ping_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health._ping_broker_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health.ping_database", lambda: None)
    monkeypatch.setattr("apps.api.app.routers.health._check_migration_drift", lambda: False)
    monkeypatch.setattr("apps.api.app.routers.health._check_outbox_health", lambda: {"status": "ok", "pending_count": 0})
    monkeypatch.setattr(
        "apps.api.app.routers.health.get_settings",
        lambda: type(
            "S",
            (),
            {
                "metrics_token": "secret",
                "app_env": "test",
                "rate_limit_fail_closed": False,
                "rate_limit_degraded_memory_fallback": False,
                "massive_api_key": None,
                "sentry_dsn": None,
                "redis_cache_url": None,
                "option_cache_enabled": False,
            },
        )(),
    )

    resp = client.get("/health/ready", headers={"x-metrics-token": "secret"})

    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["migration_aligned"] is False


def test_health_ready_returns_503_when_outbox_is_stale(client, monkeypatch):
    monkeypatch.setattr("apps.api.app.routers.health.ping_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health._ping_broker_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health.ping_database", lambda: None)
    monkeypatch.setattr("apps.api.app.routers.health._check_migration_drift", lambda: True)
    monkeypatch.setattr(
        "apps.api.app.routers.health._check_outbox_health",
        lambda: {"status": "stale", "pending_count": 5, "oldest_pending_age_seconds": 601.0},
    )
    monkeypatch.setattr(
        "apps.api.app.routers.health.get_settings",
        lambda: type(
            "S",
            (),
            {
                "metrics_token": "secret",
                "app_env": "test",
                "rate_limit_fail_closed": False,
                "rate_limit_degraded_memory_fallback": False,
                "massive_api_key": None,
                "sentry_dsn": None,
                "redis_cache_url": None,
                "option_cache_enabled": False,
            },
        )(),
    )

    resp = client.get("/health/ready", headers={"x-metrics-token": "secret"})

    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["outbox"]["status"] == "stale"
