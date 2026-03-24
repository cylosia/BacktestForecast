from __future__ import annotations

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock


class _FakeGauge:
    def __init__(self) -> None:
        self.values: dict[str, float] = {}

    def labels(self, *, pool: str):
        gauge = self

        class _BoundGauge:
            def set(self, value: float) -> None:
                gauge.values[pool] = value

        return _BoundGauge()


def test_worker_redis_connection_metric_reports_open_pools(monkeypatch) -> None:
    from apps.worker.app import celery_app as module

    gauge = _FakeGauge()
    monkeypatch.setattr(module, "WORKER_REDIS_OPEN_CONNECTIONS", gauge)
    monkeypatch.setattr(module, "_heartbeat_redis_open_connections", 1)
    monkeypatch.setattr(
        "apps.worker.app.task_base.get_dlq_redis_open_connection_count",
        lambda: 2,
    )
    monkeypatch.setattr(
        "backtestforecast.events.get_redis_open_connection_count",
        lambda: 3,
    )

    module._set_worker_redis_connection_metrics()

    assert gauge.values == {"heartbeat": 1, "events": 3, "dlq": 2}


def test_worker_shutdown_closes_resources_and_resets_redis_metrics(monkeypatch) -> None:
    from apps.worker.app import celery_app as module

    gauge = _FakeGauge()
    metrics_server = MagicMock()
    heartbeat_redis = MagicMock()
    engine = MagicMock()
    shutdown_redis = MagicMock()

    def fake_from_url(*args, **kwargs):
        return heartbeat_redis

    def fake_get_worker_engine():
        return engine

    fake_get_worker_engine.cache_info = lambda: SimpleNamespace(currsize=1)

    monkeypatch.setattr(module, "WORKER_REDIS_OPEN_CONNECTIONS", gauge)
    monkeypatch.setattr(module, "_metrics_server", metrics_server)
    monkeypatch.setattr(module, "_worker_heartbeat_key", "worker:heartbeat:test")
    monkeypatch.setattr(module, "_heartbeat_stop", threading.Event())
    monkeypatch.setattr(module, "_heartbeat_redis_open_connections", 1)
    monkeypatch.setattr("redis.Redis.from_url", fake_from_url)
    monkeypatch.setattr("backtestforecast.db.session._get_worker_engine", fake_get_worker_engine)
    monkeypatch.setattr("backtestforecast.events._shutdown_redis", shutdown_redis)

    module._on_worker_shutdown()

    metrics_server.shutdown.assert_called_once_with()
    heartbeat_redis.delete.assert_called_once_with("worker:heartbeat:test")
    heartbeat_redis.close.assert_called_once_with()
    engine.dispose.assert_called_once_with()
    shutdown_redis.assert_called_once_with()
    assert gauge.values == {"heartbeat": 0, "events": 0, "dlq": 0}
    assert module._heartbeat_stop.is_set() is True
