from __future__ import annotations

from unittest.mock import patch


def test_health_live_is_never_rate_limited(client):
    for _ in range(400):
        resp = client.get("/health/live")
        assert resp.status_code == 200


def test_health_ready_is_never_rate_limited(client, monkeypatch):
    monkeypatch.setattr("apps.api.app.routers.health.ping_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health._ping_broker_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health._ping_result_backend_redis", lambda: True)
    with patch("apps.api.app.routers.health.ping_database"):
        for _ in range(200):
            resp = client.get("/health/ready")
            assert resp.status_code in (200, 503)


def test_health_ready_returns_503_when_migration_drift_detected(client, monkeypatch):
    monkeypatch.setattr("apps.api.app.routers.health.ping_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health._ping_broker_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health._ping_result_backend_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health.ping_database", lambda: None)
    monkeypatch.setattr("apps.api.app.routers.health._get_migration_status", lambda: {
        "aligned": False,
        "applied_revision": "20260328_0009",
        "expected_revision": "20260328_0010",
    })
    monkeypatch.setattr(
        "apps.api.app.routers.health._get_operations_status",
        lambda **kwargs: {"outbox": {"status": "ok", "pending_count": 0}, "queue_diagnostics": {"status": "ok"}},
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
    assert body["migration_aligned"] is False
    assert body["migration_applied_revision"] == "20260328_0009"
    assert body["migration_expected_revision"] == "20260328_0010"


def test_health_ready_returns_503_for_migration_drift_without_metrics_token(client, monkeypatch):
    monkeypatch.setattr("apps.api.app.routers.health.ping_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health._ping_broker_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health._ping_result_backend_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health.ping_database", lambda: None)
    monkeypatch.setattr("apps.api.app.routers.health._get_migration_status", lambda: {
        "aligned": False,
        "applied_revision": "20260328_0009",
        "expected_revision": "20260328_0010",
    })
    monkeypatch.setattr(
        "apps.api.app.routers.health._get_operations_status",
        lambda **kwargs: {"outbox": {"status": "ok", "pending_count": 0}, "queue_diagnostics": {"status": "ok"}},
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

    resp = client.get("/health/ready")

    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert "migration_aligned" not in body


def test_health_ready_returns_503_when_outbox_is_stale(client, monkeypatch):
    monkeypatch.setattr("apps.api.app.routers.health.ping_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health._ping_broker_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health._ping_result_backend_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health.ping_database", lambda: None)
    monkeypatch.setattr("apps.api.app.routers.health._get_migration_status", lambda: {
        "aligned": True,
        "applied_revision": "20260328_0010",
        "expected_revision": "20260328_0010",
    })
    monkeypatch.setattr(
        "apps.api.app.routers.health._get_operations_status",
        lambda: {
            "outbox": {
                "status": "stale",
                "pending_count": 5,
                "oldest_pending_age_seconds": 601.0,
            },
            "queue_diagnostics": {"status": "ok"},
        },
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


def test_health_ready_returns_503_when_result_backend_is_down(client, monkeypatch):
    monkeypatch.setattr("apps.api.app.routers.health.ping_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health._ping_broker_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health._ping_result_backend_redis", lambda: False)
    monkeypatch.setattr("apps.api.app.routers.health.ping_database", lambda: None)
    monkeypatch.setattr("apps.api.app.routers.health._get_migration_status", lambda: {
        "aligned": True,
        "applied_revision": "20260330_0013",
        "expected_revision": "20260330_0013",
    })
    monkeypatch.setattr(
        "apps.api.app.routers.health._get_operations_status",
        lambda **kwargs: {"outbox": {"status": "ok", "pending_count": 0}, "queue_diagnostics": {"status": "ok"}},
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
                "celery_result_backend_url": "redis://localhost:6379/2",
            },
        )(),
    )

    resp = client.get("/health/ready", headers={"x-metrics-token": "secret"})

    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["result_backend"] == "down"
