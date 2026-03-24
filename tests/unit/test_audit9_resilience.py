"""Resilience and operational tests for audit round 9 fixes."""
from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestReaperHeartbeatGuard:
    """Fix #8: Reaper checks last_heartbeat_at before failing running jobs."""

    def test_heartbeat_column_exists_on_models(self):
        from backtestforecast.models import (
            BacktestRun,
            ExportJob,
            ScannerJob,
            SweepJob,
            SymbolAnalysis,
        )

        for model in (BacktestRun, ExportJob, ScannerJob, SweepJob, SymbolAnalysis):
            assert hasattr(model, "last_heartbeat_at"), (
                f"{model.__name__} missing last_heartbeat_at"
            )


class TestHeartbeatHelper:
    """Fix #9: Heartbeat helper function exists in tasks."""

    def test_update_heartbeat_exists(self):
        import os

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "dummy"}, clear=False):
            from apps.worker.app.tasks import _update_heartbeat

        assert callable(_update_heartbeat)


class TestDispatchEnqueueFailedRecovery:
    """Fix #25: dispatch_celery_task handles all retries exhausted."""

    def test_all_send_attempts_fail(self):
        import os

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "dummy"}, clear=False):
            from apps.api.app.dispatch import DispatchResult, dispatch_celery_task
            from backtestforecast.schemas.common import RunJobStatus

        job = MagicMock()
        job.status = RunJobStatus.QUEUED
        job.celery_task_id = None
        db = MagicMock()

        with patch("apps.worker.app.celery_app.celery_app") as mock_celery:
            mock_celery.send_task.side_effect = ConnectionError("broker down")
            result = dispatch_celery_task(
                db=db,
                job=job,
                task_name="test.task",
                task_kwargs={"id": "test"},
                queue="test",
                log_event="test",
                logger=MagicMock(),
            )

        assert result == DispatchResult.ENQUEUE_FAILED
        assert job.status == RunJobStatus.QUEUED
        assert job.celery_task_id is not None


class TestCircuitBreakerClusterPropagation:
    """Fix #16: Circuit breaker checks cluster state every 2 seconds."""

    def test_redis_check_interval(self):
        from backtestforecast.resilience.circuit_breaker import CircuitBreaker

        cb = CircuitBreaker("test_service")
        assert cb._redis_check_interval == 2.0

    def test_cluster_state_checked_on_allow_request(self):
        from backtestforecast.resilience.circuit_breaker import CircuitBreaker

        mock_redis = MagicMock()
        mock_redis.get.return_value = b"10"
        cb = CircuitBreaker("test", failure_threshold=5, redis_client=mock_redis)
        cb._last_redis_check = 0.0

        result = cb.allow_request()
        assert result is False  # Should be open due to cluster state


class TestWorkerMetricsBind:
    """Fix #27: Worker metrics bind to 127.0.0.1 in production."""

    def test_prod_compose_has_metrics_bind(self):
        import pathlib

        compose = pathlib.Path("docker-compose.prod.yml")
        if compose.exists():
            content = compose.read_text()
            assert "WORKER_METRICS_BIND" in content
            assert "127.0.0.1" in content


class TestBacktestTradeIndex:
    """Fix #18: backtest_trades has index on run_id."""

    def test_run_id_index_exists(self):
        from backtestforecast.models import BacktestTrade

        index_names = [
            idx.name for idx in BacktestTrade.__table_args__ if hasattr(idx, "name")
        ]
        assert "ix_backtest_trades_run_id" in index_names
