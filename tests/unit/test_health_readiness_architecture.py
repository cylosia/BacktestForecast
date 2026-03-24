from __future__ import annotations

from types import SimpleNamespace


def test_ready_reports_operations_status_only_in_detailed_mode(monkeypatch) -> None:
    from apps.api.app.routers import health

    calls = {"ops": 0}

    monkeypatch.setattr(health, "ping_database", lambda: None)
    monkeypatch.setattr(health, "ping_redis", lambda: True)
    monkeypatch.setattr(health, "_ping_broker_redis", lambda: True)
    monkeypatch.setattr(health, "_check_massive_health", lambda settings: "ok")
    monkeypatch.setattr(health, "_check_migration_drift", lambda: True)
    monkeypatch.setattr(
        health,
        "_get_operations_status",
        lambda **kwargs: calls.__setitem__("ops", calls["ops"] + 1) or {"outbox": {"status": "ok"}, "queue_diagnostics": {"status": "ok"}},
    )
    monkeypatch.setattr(
        health,
        "get_settings",
        lambda: SimpleNamespace(
            metrics_token="secret",
            app_env="test",
            rate_limit_fail_closed=True,
            rate_limit_degraded_memory_fallback=False,
            sentry_dsn=None,
            option_cache_enabled=False,
            redis_cache_url=None,
        ),
    )

    no_details = health.ready(SimpleNamespace(headers={}))
    with_details = health.ready(SimpleNamespace(headers={"x-metrics-token": "secret"}))

    assert no_details.status_code == 200
    assert with_details.status_code == 200
    assert calls["ops"] == 1


def test_ready_stays_degraded_but_available_when_migrations_drift(monkeypatch) -> None:
    from apps.api.app.routers import health

    monkeypatch.setattr(health, "ping_database", lambda: None)
    monkeypatch.setattr(health, "ping_redis", lambda: True)
    monkeypatch.setattr(health, "_ping_broker_redis", lambda: True)
    monkeypatch.setattr(health, "_check_massive_health", lambda settings: "ok")
    monkeypatch.setattr(health, "_check_migration_drift", lambda: False)
    monkeypatch.setattr(
        health,
        "get_settings",
        lambda: SimpleNamespace(
            metrics_token=None,
            app_env="production",
            rate_limit_fail_closed=False,
            rate_limit_degraded_memory_fallback=True,
            sentry_dsn=None,
            option_cache_enabled=False,
            redis_cache_url=None,
        ),
    )

    response = health.ready(SimpleNamespace(headers={}))

    assert response.status_code == 200


def test_ready_skips_migration_check_without_detailed_access(monkeypatch) -> None:
    from apps.api.app.routers import health

    monkeypatch.setattr(health, "ping_database", lambda: None)
    monkeypatch.setattr(health, "ping_redis", lambda: True)
    monkeypatch.setattr(health, "_ping_broker_redis", lambda: True)
    monkeypatch.setattr(health, "_check_massive_health", lambda settings: "ok")
    monkeypatch.setattr(
        health,
        "_check_migration_drift",
        lambda: (_ for _ in ()).throw(AssertionError("migration drift should not be checked without detailed access")),
    )
    monkeypatch.setattr(
        health,
        "get_settings",
        lambda: SimpleNamespace(
            metrics_token="secret",
            app_env="production",
            rate_limit_fail_closed=False,
            rate_limit_degraded_memory_fallback=True,
            sentry_dsn=None,
            option_cache_enabled=False,
            redis_cache_url=None,
        ),
    )

    response = health.ready(SimpleNamespace(headers={}))

    assert response.status_code == 200


def test_queue_health_includes_broker_recovery_depth(monkeypatch) -> None:
    from apps.api.app.routers import health

    monkeypatch.setattr(health, "_get_broker_queue_depths", lambda: {"maintenance": 0, "recovery": 3})

    class _FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

        def rollback(self):
            return None

    monkeypatch.setattr("backtestforecast.db.session.create_session", lambda: _FakeSession())
    monkeypatch.setattr(
        health,
        "get_queue_diagnostics",
        lambda session: {"status": "ok", "stale_queued_total": 0, "stale_without_outbox_total": 0, "models": {}},
    )

    payload = health._check_queue_health()

    assert payload["broker_queue_depths"] == {"maintenance": 0, "recovery": 3}
    assert payload["status"] == "recovery_backlog"
