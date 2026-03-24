"""Tests covering the 10 testing gaps identified in the production audit.

Each test validates a specific fix that was applied during the audit.
"""
from __future__ import annotations

import inspect
import threading
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# TG-1: ScanService.delete_for_user must not crash on missing symbol column
# ---------------------------------------------------------------------------

class TestScanDeleteForUser:
    def _make_scan_service(self, *, job_status="succeeded", request_snapshot=None):
        from backtestforecast.services.scans import ScanService

        if request_snapshot is None:
            request_snapshot = {"symbols": ["AAPL", "MSFT"], "mode": "basic"}
        mock_job = MagicMock()
        mock_job.id = uuid.uuid4()
        mock_job.status = job_status
        mock_job.mode = "basic"
        mock_job.request_snapshot_json = request_snapshot

        service = ScanService.__new__(ScanService)
        service.session = MagicMock()
        service.repository = MagicMock()
        service.repository.get_for_user = MagicMock(return_value=mock_job)
        service.audit = MagicMock()
        service.audit.record = MagicMock()
        return service, mock_job

    def test_delete_succeeds_without_symbol_column(self):
        service, job = self._make_scan_service()
        service.delete_for_user(job.id, uuid.uuid4())
        service.session.delete.assert_called_once_with(job)

    def test_delete_passes_symbols_from_snapshot(self):
        service, job = self._make_scan_service(
            request_snapshot={"symbols": ["TSLA", "NVDA", "AMD"], "mode": "advanced"}
        )
        user_id = uuid.uuid4()
        service.delete_for_user(job.id, user_id)
        call_kwargs = service.audit.record.call_args[1]
        assert call_kwargs["metadata"]["symbols"] == ["TSLA", "NVDA", "AMD"]

    def test_delete_handles_empty_snapshot(self):
        service, _ = self._make_scan_service(request_snapshot={})
        service.delete_for_user(service.repository.get_for_user.return_value.id, uuid.uuid4())
        call_kwargs = service.audit.record.call_args[1]
        assert call_kwargs["metadata"]["symbols"] == []

    def test_delete_rejects_queued_job(self):
        from backtestforecast.errors import ConflictError

        service, job = self._make_scan_service(job_status="queued")
        with pytest.raises(ConflictError):
            service.delete_for_user(job.id, uuid.uuid4())


# ---------------------------------------------------------------------------
# TG-2: Sweep progress commits use savepoints (don't release parent lock)
# ---------------------------------------------------------------------------

class TestSweepProgressSavepoint:
    def test_heartbeat_uses_savepoint(self):
        from backtestforecast.models import SweepJob
        from backtestforecast.services.sweeps import _update_heartbeat

        session = MagicMock()
        nested = MagicMock()
        session.begin_nested.return_value = nested

        _update_heartbeat(session, SweepJob, uuid.uuid4())

        session.begin_nested.assert_called_once()
        nested.commit.assert_called_once()

    def test_heartbeat_rollback_on_failure(self):
        from backtestforecast.models import SweepJob
        from backtestforecast.services.sweeps import _update_heartbeat

        session = MagicMock()
        nested = MagicMock()
        session.begin_nested.return_value = nested
        session.execute.side_effect = RuntimeError("DB gone")

        _update_heartbeat(session, SweepJob, uuid.uuid4())

        nested.rollback.assert_called_once()

    def test_heartbeat_failure_counter_increments(self):
        from backtestforecast.models import SweepJob
        from backtestforecast.services.sweeps import _update_heartbeat
        import backtestforecast.services.sweep_service_helpers as sweep_helpers

        original = sweep_helpers._heartbeat_failures
        try:
            sweep_helpers._heartbeat_failures = 0
            session = MagicMock()
            session.begin_nested.side_effect = RuntimeError("fail")

            _update_heartbeat(session, SweepJob, uuid.uuid4())
            assert sweep_helpers._heartbeat_failures == 1

            _update_heartbeat(session, SweepJob, uuid.uuid4())
            assert sweep_helpers._heartbeat_failures == 2
        finally:
            sweep_helpers._heartbeat_failures = original


# ---------------------------------------------------------------------------
# TG-3: DLQ read/write URL consistency
# ---------------------------------------------------------------------------

class TestDlqUrlConsistency:
    def test_worker_dlq_uses_cache_url(self):
        source = (_ROOT / "apps" / "worker" / "app" / "task_base.py").read_text()
        assert "redis_cache_url" in source, (
            "Worker DLQ must use redis_cache_url to match the API DLQ reader"
        )

    def test_api_dlq_uses_cache_url(self):
        source = (_ROOT / "apps" / "api" / "app" / "main.py").read_text()
        fn_start = source.find("def _get_dlq_redis")
        assert fn_start != -1, "_get_dlq_redis not found in main.py"
        fn_body = source[fn_start:fn_start + 500]
        assert "redis_cache_url" in fn_body, (
            "API DLQ reader must use redis_cache_url to match the worker DLQ writer"
        )


# ---------------------------------------------------------------------------
# TG-4: Billing webhook NotFoundError triggers Stripe retry (500, not 200)
# ---------------------------------------------------------------------------

class TestWebhookNotFoundRetry:
    def test_not_found_error_returns_500_not_200(self):
        source = (_ROOT / "apps" / "api" / "app" / "routers" / "billing.py").read_text()
        nfe_block_start = source.find("isinstance(exc, _NotFoundErr)")
        assert nfe_block_start != -1, "NotFoundError handler not found in billing router"
        nfe_block = source[nfe_block_start:nfe_block_start + 400]
        assert "status_code=500" in nfe_block, (
            "NotFoundError in webhook must return 500 so Stripe retries"
        )
        assert "WebhookResponse(received=True)" not in nfe_block, (
            "NotFoundError must NOT return 200 WebhookResponse (would prevent Stripe retry)"
        )


# ---------------------------------------------------------------------------
# TG-5: Monthly quota excludes failed/cancelled jobs
# ---------------------------------------------------------------------------

class TestQuotaExcludesFailedJobs:
    def test_backtest_quota_filters_failed(self):
        from backtestforecast.repositories.backtest_runs import BacktestRunRepository

        source = __import__("inspect").getsource(
            BacktestRunRepository.count_for_user_created_between
        )
        assert 'notin_' in source and 'failed' in source

    def test_sweep_quota_filters_failed(self):
        from backtestforecast.services.sweeps import SweepService

        source = __import__("inspect").getsource(SweepService._enforce_sweep_quota)
        assert 'notin_' in source and 'failed' in source


# ---------------------------------------------------------------------------
# TG-6: Concurrent sweep creation serialized by FOR UPDATE on User row
# ---------------------------------------------------------------------------

class TestSweepQuotaSerialization:
    def test_enforce_sweep_quota_uses_for_update(self):
        from backtestforecast.services.sweeps import SweepService

        source = __import__("inspect").getsource(SweepService._enforce_sweep_quota)
        assert "with_for_update" in source, (
            "_enforce_sweep_quota must lock the User row to prevent concurrent quota bypass"
        )


# ---------------------------------------------------------------------------
# TG-7: Export expiration lifecycle
# ---------------------------------------------------------------------------

class TestExportExpirationLifecycle:
    def test_to_response_shows_expired_for_past_expires_at(self):
        from backtestforecast.services.exports import ExportService

        job = MagicMock()
        job.id = uuid.uuid4()
        job.backtest_run_id = uuid.uuid4()
        job.export_format = "csv"
        job.status = "succeeded"
        job.file_name = "test.csv"
        job.mime_type = "text/csv"
        job.size_bytes = 100
        job.sha256_hex = "abc123"
        job.error_code = None
        job.error_message = None
        job.created_at = datetime.now(UTC) - timedelta(days=31)
        job.started_at = datetime.now(UTC) - timedelta(days=31)
        job.completed_at = datetime.now(UTC) - timedelta(days=31)
        job.expires_at = datetime.now(UTC) - timedelta(days=1)

        response = ExportService.to_response(job)
        assert response.status == "expired", (
            "to_response must show 'expired' when expires_at is in the past"
        )

    def test_to_response_shows_succeeded_before_expiry(self):
        from backtestforecast.services.exports import ExportService

        job = MagicMock()
        job.id = uuid.uuid4()
        job.backtest_run_id = uuid.uuid4()
        job.export_format = "csv"
        job.status = "succeeded"
        job.file_name = "test.csv"
        job.mime_type = "text/csv"
        job.size_bytes = 100
        job.sha256_hex = "abc123"
        job.error_code = None
        job.error_message = None
        job.created_at = datetime.now(UTC)
        job.started_at = datetime.now(UTC)
        job.completed_at = datetime.now(UTC)
        job.expires_at = datetime.now(UTC) + timedelta(days=29)

        response = ExportService.to_response(job)
        assert response.status == "succeeded"

    def test_expire_task_registered_in_beat_schedule(self):
        source = (_ROOT / "apps" / "worker" / "app" / "celery_app.py").read_text()
        assert "expire_old_exports" in source, (
            "expire_old_exports must be referenced in celery_app.py"
        )
        assert "maintenance.expire_old_exports" in source, (
            "maintenance.expire_old_exports task must be in beat_schedule"
        )


# ---------------------------------------------------------------------------
# TG-8: Scan candidate memory cap
# ---------------------------------------------------------------------------

class TestScanMemoryCap:
    def test_candidate_heap_is_bounded(self):
        from backtestforecast.services.scans import ScanService

        assert ScanService._MIN_TOP_CANDIDATE_BUFFER <= 2000, (
            "Top-candidate buffer must be bounded to prevent OOM"
        )
        assert ScanService._MIN_TOP_CANDIDATE_BUFFER >= 10, (
            "Top-candidate buffer must be large enough for meaningful scans"
        )
        source = inspect.getsource(ScanService._execute_scan)
        assert "keep_limit = max(payload.max_recommendations * 3, self._MIN_TOP_CANDIDATE_BUFFER)" in source


# ---------------------------------------------------------------------------
# TG-9: _D() precision with edge-case floats
# ---------------------------------------------------------------------------

class TestDecimalConversionPrecision:
    def test_zero(self):
        from backtestforecast.backtests.engine import _D, _D0

        assert _D(0) is _D0
        assert _D(0.0) is _D0

    def test_common_values_cached(self):
        from backtestforecast.backtests.engine import _D

        assert _D(1) == Decimal("1")
        assert _D(100) == Decimal("100")
        assert _D(-1) == Decimal("-1")

    def test_float_precision_preserved(self):
        from backtestforecast.backtests.engine import _D

        result = _D(3.14)
        assert result == Decimal("3.14")
        assert str(result) == "3.14"

    def test_large_float(self):
        from backtestforecast.backtests.engine import _D

        result = _D(100000.5678)
        assert result == Decimal("100000.5678")

    def test_small_float(self):
        from backtestforecast.backtests.engine import _D

        result = _D(0.0001)
        assert result == Decimal("0.0001")

    def test_cache_bounded(self):
        from backtestforecast.backtests.engine import _D, _D_CACHE, _D_CACHE_MAX

        len(_D_CACHE)
        for i in range(10):
            _D(float(i) + 0.123456789)
        assert len(_D_CACHE) <= _D_CACHE_MAX

    def test_negative_float(self):
        from backtestforecast.backtests.engine import _D

        result = _D(-42.5)
        assert result == Decimal("-42.5")

    def test_repeated_call_returns_same_value(self):
        from backtestforecast.backtests.engine import _D

        a = _D(1.23)
        b = _D(1.23)
        assert a == b


# ---------------------------------------------------------------------------
# TG-10: Settings invalidation with RLock (no deadlock)
# ---------------------------------------------------------------------------

class TestSettingsInvalidationSafety:
    def test_settings_lock_is_reentrant(self):
        from backtestforecast.config import _settings_lock

        assert isinstance(_settings_lock, type(threading.RLock())), (
            "Settings lock must be RLock to prevent deadlocks from nested access"
        )

    def test_invalidation_does_not_deadlock(self):
        import warnings as _warnings

        from backtestforecast.config import (
            get_settings,
            invalidate_settings,
            register_invalidation_callback,
        )

        callback_called = threading.Event()

        def nested_callback():
            with _warnings.catch_warnings():
                _warnings.simplefilter("ignore")
                get_settings()
            callback_called.set()

        register_invalidation_callback(nested_callback)

        result = [None]

        def run_invalidation():
            try:
                invalidate_settings()
                result[0] = "ok"
            except Exception as exc:
                result[0] = f"error: {exc}"

        t = threading.Thread(target=run_invalidation)
        t.start()
        t.join(timeout=5)

        assert not t.is_alive(), "invalidate_settings deadlocked"
        assert result[0] == "ok", f"invalidate_settings failed: {result[0]}"
        assert callback_called.is_set(), "Nested callback was not executed"

    def test_callback_limit_prevents_unbounded_growth(self):
        from backtestforecast.config import (
            _MAX_INVALIDATION_CALLBACKS,
            _invalidation_callbacks,
            _settings_lock,
        )

        assert _MAX_INVALIDATION_CALLBACKS >= 50, "Callback limit should be reasonable"
        with _settings_lock:
            current = len(_invalidation_callbacks)
        assert current < _MAX_INVALIDATION_CALLBACKS, (
            f"Already at {current}/{_MAX_INVALIDATION_CALLBACKS} callbacks"
        )
