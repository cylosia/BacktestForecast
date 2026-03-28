from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import json
import pytest

from backtestforecast.errors import AppValidationError
from backtestforecast.models import User


def test_dispatch_targets_and_celery_routes_use_isolated_queues() -> None:
    from apps.worker.app.celery_app import celery_app
    from backtestforecast.services.dispatch_recovery import DISPATCH_TARGETS

    target_queues = {target.task_name: target.queue for target in DISPATCH_TARGETS}
    assert target_queues["backtests.run"] == "backtests"
    assert target_queues["scans.run_job"] == "scans"
    assert target_queues["sweeps.run"] == "sweeps"
    assert target_queues["analysis.deep_symbol"] == "analysis"

    routes = celery_app.conf.task_routes
    assert routes["backtests.run"]["queue"] == "backtests"
    assert routes["scans.run_job"]["queue"] == "scans"
    assert routes["sweeps.run"]["queue"] == "sweeps"
    assert routes["analysis.deep_symbol"]["queue"] == "analysis"
    assert routes["maintenance.poll_outbox"]["queue"] == "recovery"
    assert routes["maintenance.reconcile_subscriptions"]["queue"] == "recovery"
    assert routes["maintenance.cleanup_stripe_orphan"]["queue"] == "recovery"
    assert routes["maintenance.reconcile_stranded_jobs"]["queue"] == "recovery"


def test_production_worker_topology_separates_heavy_queues() -> None:
    source = Path("docker-compose.prod.yml").read_text()
    assert "--queues=backtests,scans,exports,research" in source
    assert "--queues=sweeps,analysis" in source
    assert "--queues=pipeline" in source
    assert "--queues=maintenance --concurrency=2" in source
    assert "--queues=recovery --concurrency=2" in source
    assert "/health/live" in source
    assert "--destination=celery@$HOSTNAME" in source


def test_api_image_healthcheck_uses_live_endpoint() -> None:
    source = Path("apps/api/Dockerfile").read_text()
    assert "/health/live" in source
    assert "/health/ready" not in source[source.index("HEALTHCHECK"):]


def test_root_endpoint_advertises_live_health_path() -> None:
    source = Path("apps/api/app/main.py").read_text()
    root_block = source[source.index('def root()'):source.index('if get_settings().app_env in ("development", "test"):')]
    assert '"health": "/health/live"' in root_block


def test_production_topology_uses_redundant_beat_replicas() -> None:
    source = Path("docker-compose.prod.yml").read_text()
    assert "beat:" in source
    beat_block = source[source.index("beat:"):source.index("web:")]
    assert "replicas: 2" in beat_block


def test_reaper_observability_tracks_recovery_queue_depth() -> None:
    source = Path("apps/worker/app/tasks.py").read_text()
    assert '"recovery"' in source[source.index('for q_name in ('):source.index('QUEUE_DEPTH.labels(queue=q_name).set(depth)')]


def test_reaper_queued_redispatch_uses_transactional_dispatch() -> None:
    source = Path("apps/worker/app/tasks.py").read_text()
    start = source.index("def _reap_queued_jobs")
    end = source.index("def _fail_stale_running_jobs")
    method_source = source[start:end]

    assert "dispatch_celery_task" in method_source
    assert "celery_app.send_task" not in method_source


def test_reaper_uses_database_lock_fallback_when_redis_lock_is_unavailable() -> None:
    source = Path("apps/worker/app/tasks.py").read_text()
    reap_block = source[source.index("def reap_stale_jobs"):source.index("def reconcile_stranded_jobs")]
    assert "pg_try_advisory_lock" in reap_block
    assert "reaper.db_lock_unavailable" in reap_block


def test_stale_claim_recovery_does_not_clear_claim_when_backend_probe_is_uncertain() -> None:
    source = Path("apps/worker/app/tasks.py").read_text()
    start = source.index("orphan_cutoff = datetime.now(UTC)")
    end = source.index("try:\n        from sqlalchemy import text")
    block = source[start:end]
    assert "probe_uncertain = True" in block
    assert "if probe_uncertain:" in block


def test_large_db_backed_export_download_is_blocked_before_loading_bytes(monkeypatch) -> None:
    from apps.api.app.routers import exports as exports_router

    called = {"db_bytes": 0}

    class _FakeExportService:
        def __init__(self, db):
            self.db = db

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

        def get_export_for_download(self, user, export_job_id, request_id=None, ip_address=None):
            return SimpleNamespace(
                id=export_job_id,
                file_name="report.csv",
                mime_type="text/csv",
                size_bytes=exports_router._MAX_DB_INLINE_DOWNLOAD_BYTES + 1,
                storage_key=None,
                sha256_hex=None,
            )

        def get_db_content_bytes_for_download(self, user, export_job_id):
            called["db_bytes"] += 1
            return b"should not be loaded"

    monkeypatch.setattr(exports_router, "ExportService", _FakeExportService)
    monkeypatch.setattr(
        exports_router,
        "get_rate_limiter",
        lambda: SimpleNamespace(check=lambda **kwargs: None),
    )

    user = User(clerk_user_id="export-user", email="export@example.com")
    user.id = uuid4()

    with pytest.raises(AppValidationError):
        exports_router.download_export(
            export_job_id=uuid4(),
            request=SimpleNamespace(),
            user=user,
            metadata=SimpleNamespace(request_id="req-1", ip_address="127.0.0.1"),
            db=object(),
            settings=SimpleNamespace(export_create_rate_limit=5, rate_limit_window_seconds=60),
        )

    assert called["db_bytes"] == 0


def test_database_storage_export_limit_matches_api_download_limit() -> None:
    from apps.api.app.routers.exports import _MAX_DB_INLINE_DOWNLOAD_BYTES
    from backtestforecast.services.exports import MAX_DATABASE_DOWNLOADABLE_EXPORT_BYTES

    assert MAX_DATABASE_DOWNLOADABLE_EXPORT_BYTES == _MAX_DB_INLINE_DOWNLOAD_BYTES


@pytest.mark.asyncio
async def test_sse_slot_acquire_fails_closed_in_production_when_redis_is_unavailable(monkeypatch) -> None:
    from apps.api.app.routers import events

    async def _boom():
        raise ConnectionError("redis unavailable")

    async def _fallback(_user_id):
        raise AssertionError("production should not use in-process fallback")

    monkeypatch.setattr(events, "_get_async_redis", _boom)
    monkeypatch.setattr(events, "_acquire_sse_slot_in_process", _fallback)
    monkeypatch.setattr(events, "get_settings", lambda: SimpleNamespace(app_env="production"))

    acquired, used_redis = await events._acquire_sse_slot(uuid4())

    assert acquired is False
    assert used_redis is False


def test_ready_stays_healthy_when_broker_is_down_if_database_is_up(monkeypatch) -> None:
    from apps.api.app.routers import health

    monkeypatch.setattr(health, "ping_database", lambda: None)
    monkeypatch.setattr(health, "get_missing_schema_tables", lambda: ())
    monkeypatch.setattr(health, "ping_redis", lambda: True)
    monkeypatch.setattr(health, "_ping_broker_redis", lambda: False)
    monkeypatch.setattr(health, "_check_massive_health", lambda settings: "ok")
    monkeypatch.setattr(health, "_check_migration_drift", lambda: True)
    monkeypatch.setattr(health, "_get_operations_status", lambda **kwargs: (_ for _ in ()).throw(AssertionError("deep checks should not run")))
    monkeypatch.setattr(
        health,
        "get_settings",
        lambda: SimpleNamespace(
            metrics_token=None,
            app_env="test",
            rate_limit_fail_closed=True,
            rate_limit_degraded_memory_fallback=False,
            sentry_dsn=None,
        ),
    )
    response = health.ready(SimpleNamespace(headers={}))

    assert response.status_code == 200
    payload = json.loads(response.body)
    assert payload["status"] == "degraded"


def test_refresh_prioritized_scans_uses_transactional_dispatch(monkeypatch) -> None:
    from apps.worker.app import tasks as tasks_module

    dispatch_calls: list[dict[str, object]] = []

    class _FakeLock:
        def acquire(self, blocking=False):
            return True

        def release(self):
            return None

    class _FakeRedis:
        def lock(self, *_args, **_kwargs):
            return _FakeLock()

        def close(self):
            return None

    class _FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

        def rollback(self):
            return None

    class _FakeScanService:
        def __init__(self, session):
            self.session = session

        def create_and_dispatch_scheduled_refresh_jobs(self, *, limit=25, dispatch_logger=None):
            dispatch_calls.append({"limit": limit, "dispatch_logger": dispatch_logger})
            return (1, 1)

    monkeypatch.setattr("backtestforecast.utils.create_cache_redis", lambda decode_responses=False: _FakeRedis())
    monkeypatch.setattr(tasks_module, "create_worker_session", lambda: _FakeSession())
    monkeypatch.setattr(tasks_module, "ScanService", _FakeScanService)
    monkeypatch.setattr(tasks_module.celery_app, "send_task", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("direct send_task should not be used")))
    result = tasks_module.refresh_prioritized_scans.run()

    assert dispatch_calls == [{"limit": 25, "dispatch_logger": tasks_module.logger}]
    assert result == {"scheduled_jobs": 1, "pending_recovery": 1}


def test_create_scheduled_refresh_jobs_does_not_commit_before_dispatch() -> None:
    source = Path("src/backtestforecast/services/scans.py").read_text()
    start = source.index("def create_scheduled_refresh_jobs")
    end = source.index("def create_and_dispatch_scheduled_refresh_jobs")
    method_source = source[start:end]

    assert "self.session.commit()" not in method_source


def test_scheduled_refresh_dispatch_creates_jobs_one_at_a_time() -> None:
    source = Path("src/backtestforecast/services/scans.py").read_text()
    start = source.index("def create_and_dispatch_scheduled_refresh_jobs")
    end = source.index("def _build_forecast_impl")
    method_source = source[start:end]

    assert "for spec in self._list_scheduled_refresh_specs" in method_source
    assert "dispatch_celery_task(" in method_source
    assert "create_scheduled_refresh_jobs(" not in method_source


def test_market_holiday_seed_uses_durable_outbox_dispatch(monkeypatch) -> None:
    from apps.api.app import dispatch as dispatch_module
    from apps.worker.app import celery_app as celery_module
    from backtestforecast.db import session as session_module

    fake_session = SimpleNamespace()
    dispatch_calls: list[dict[str, object]] = []

    class _FakeRedis:
        def exists(self, _key):
            return False

        def set(self, _key, _value, nx, ex):
            assert nx is True
            assert ex == 600
            return True

        def close(self):
            return None

    class _FakeRedisFactory:
        @staticmethod
        def from_url(*_args, **_kwargs):
            return _FakeRedis()

    def _fake_dispatch(**kwargs):
        dispatch_calls.append(kwargs)
        return dispatch_module.DispatchResult.SENT

    monkeypatch.setattr("redis.Redis", _FakeRedisFactory)
    monkeypatch.setattr(dispatch_module, "dispatch_outbox_task", _fake_dispatch)
    monkeypatch.setattr(celery_module.celery_app, "send_task", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("direct send_task should not be used")))

    class _FakeContext:
        def __enter__(self):
            return fake_session

        def __exit__(self, *exc):
            return None

    monkeypatch.setattr(session_module, "create_session", lambda: _FakeContext())

    celery_module._seed_market_holidays()

    assert len(dispatch_calls) == 1
    call = dispatch_calls[0]
    assert call["db"] is fake_session
    assert call["task_name"] == "maintenance.refresh_market_holidays"
    assert call["task_kwargs"] == {}
    assert call["queue"] == "maintenance"


def test_worker_image_healthcheck_targets_local_worker_instance() -> None:
    source = Path("apps/worker/Dockerfile").read_text()
    assert "--destination=celery@$$HOSTNAME" in source


def test_production_redis_roles_are_explicitly_separated() -> None:
    source = Path("src/backtestforecast/config.py").read_text()
    assert "require REDIS_CACHE_URL to be set explicitly" in source
    assert "require CELERY_RESULT_BACKEND_URL to be set explicitly" in source
    assert "REDIS_CACHE_URL must differ from REDIS_URL" in source
    assert "CELERY_RESULT_BACKEND_URL must differ from REDIS_URL" in source
    assert "redbeat_redis_url=settings.redis_cache_url or settings.redis_url" in Path("apps/worker/app/celery_app.py").read_text()


def test_subscription_reconciliation_uses_runtime_batch_limit_and_hourly_schedule() -> None:
    billing_source = Path("src/backtestforecast/services/billing.py").read_text()
    celery_source = Path("apps/worker/app/celery_app.py").read_text()

    assert "limit(self.settings.max_reconciliation_users)" in billing_source
    assert '"schedule": crontab(minute=0)' in celery_source
