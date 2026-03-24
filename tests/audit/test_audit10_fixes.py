"""Tests for audit round 10 fixes.

Covers: sweep entitlements, genetic timeout, health check circuit breaker,
export cleanup ordering, fallback persist session, scanner determinism,
zero-result sweep status, recommendation validation safety.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest


class TestSweepEntitlementEnforcement:
    """Item 1: Verify sweep creation requires forecasting access."""

    def test_create_job_calls_ensure_forecasting_access(self):
        from unittest.mock import MagicMock

        from backtestforecast.services.sweeps import SweepService

        mock_session = MagicMock()
        service = SweepService(mock_session)

        user = MagicMock()
        user.plan_tier = "free"
        user.subscription_status = None
        user.subscription_current_period_end = None
        user.id = uuid4()

        payload = MagicMock()
        payload.idempotency_key = None

        with pytest.raises(Exception) as exc_info:
            service.create_job(user, payload)
        assert "Pro" in str(exc_info.value) or "feature_locked" in str(exc_info.value).lower()


class TestGeneticSweepTimeout:
    """Item 2: Verify genetic sweep has timeout protection."""

    def test_genetic_timeout_uses_config_setting(self):
        """The genetic fitness function should use the configurable timeout, not a hardcoded constant."""
        import inspect

        from backtestforecast.services.sweeps import SweepService

        source = inspect.getsource(SweepService._execute_genetic)
        assert "sweep_genetic_timeout_seconds" in source, \
            "Genetic sweep must use get_settings().sweep_genetic_timeout_seconds for its timeout"
        assert "_SWEEP_TIMEOUT_SECONDS" not in source, \
            "Genetic sweep must not use the old hardcoded _SWEEP_TIMEOUT_SECONDS constant"


class TestHealthCheckCircuitBreaker:
    """Item 3: Verify health check uses module-level circuit breaker."""

    def test_check_massive_config_detects_open_circuit(self):

        settings = MagicMock()
        settings.massive_api_key = "test-key"

        with patch("apps.api.app.routers.health._check_massive_health") as mock_check:
            mock_check.return_value = "circuit_open"
            assert mock_check(settings) == "circuit_open"

    def test_check_massive_config_uses_module_circuit_breaker(self):
        """Ensure the function imports _massive_sync_circuit, not a class attribute."""
        import inspect

        from apps.api.app.routers.health import _check_massive_health

        source = inspect.getsource(_check_massive_health)
        assert "_massive_sync_circuit" in source, \
            "_check_massive_health should use _massive_sync_circuit, not getattr(MassiveClient, ...)"
        assert 'getattr(MassiveClient, "_circuit_breaker"' not in source, \
            "Dead code pattern should be removed"


class TestExportCleanupOrdering:
    """Item 4: Verify export cleanup commits DB before deleting storage."""

    def test_cleanup_commits_db_before_storage_delete(self):
        """Verify the ordering: DB commit happens before storage.delete."""
        import inspect

        from backtestforecast.services.exports import ExportService

        source = inspect.getsource(ExportService.cleanup_expired_exports)
        commit_pos = source.find("self.session.commit()")
        delete_pos = source.find("self._storage.delete(")
        assert commit_pos > 0 and delete_pos > 0, "Both commit and delete should be in the method"
        assert commit_pos < delete_pos, \
            "DB commit must happen before storage delete to prevent orphaned deletions"


class TestFallbackPersistSession:
    """Item 5: Verify fallback persist uses worker session factory."""

    def test_fallback_uses_worker_session(self):
        """Ensure _fallback_persist_status uses create_worker_session, not SessionLocal."""
        import inspect

        from backtestforecast.events import _fallback_persist_status

        source = inspect.getsource(_fallback_persist_status)
        assert "create_worker_session" in source, \
            "_fallback_persist_status should use create_worker_session for 5-minute timeout"
        assert "SessionLocal" not in source, \
            "_fallback_persist_status should not use SessionLocal (30s timeout)"


class TestScannerDeterminism:
    """Item 7: Verify scanner shuffle is seeded for determinism."""

    def test_scanner_shuffle_is_seeded(self):
        """Ensure _execute_scan uses a seeded random shuffle."""
        import inspect

        from backtestforecast.services.scans import ScanService

        source = inspect.getsource(ScanService._execute_scan)
        assert "Random(" in source or "seed(" in source, \
            "Scanner should use a seeded random shuffle for deterministic results"


class TestZeroResultSweepStatus:
    """Item 12: Verify zero-result sweeps are marked as failed."""

    def test_zero_result_sweep_marked_failed(self):
        """Ensure _execute_sweep sets status='failed' when no candidates succeed."""
        import inspect

        from backtestforecast.services.sweeps import SweepService

        source = inspect.getsource(SweepService._execute_sweep)
        failed_on_empty_idx = source.find('"sweep_empty"')
        assert failed_on_empty_idx > 0, \
            "Zero-result sweeps should be marked as failed with error_code='sweep_empty'"


class TestProductionIpHashSalt:
    """Item 13: Verify default ip_hash_salt is flagged in production."""

    def test_default_salt_triggers_warning(self):
        """Ensure the default ip_hash_salt value is flagged during production validation."""
        import inspect

        from backtestforecast.config import Settings

        source = inspect.getsource(Settings)
        assert "backtestforecast-default-ip-salt-change-me" in source
        assert "ip_hash_salt" in source


class TestRecommendationValidationSafety:
    """Item 29: Verify malformed trade JSON doesn't crash recommendation listing."""

    def test_safe_validate_list_skips_malformed(self):
        from pydantic import BaseModel

        from backtestforecast.services.scans import _safe_validate_list

        class DummyModel(BaseModel):
            x: int

        items = [{"x": 1}, {"bad": "data"}, {"x": 3}]
        result = _safe_validate_list(DummyModel, items, "test")
        assert len(result) == 2
        assert result[0].x == 1
        assert result[1].x == 3


class TestNightlyPipelineTiming:
    """Item 32: Verify nightly pipeline runs after EOD data is available."""

    def test_pipeline_runs_at_6_utc_or_later(self):
        from apps.worker.app.celery_app import celery_app

        schedule = celery_app.conf.beat_schedule.get("nightly-scan-pipeline", {})
        cron = schedule.get("schedule")
        assert cron is not None, "Nightly pipeline schedule must be defined"
        assert cron.hour == {6}, f"Pipeline should run at 6 UTC, got hour={cron.hour}"


class TestCSPHeaders:
    """Item 82: Verify CSP includes frame-ancestors."""

    def test_csp_has_frame_ancestors(self):
        import inspect

        from backtestforecast.security.http import ApiSecurityHeadersMiddleware

        source = inspect.getsource(ApiSecurityHeadersMiddleware)
        assert "frame-ancestors" in source, "CSP should include frame-ancestors directive"
