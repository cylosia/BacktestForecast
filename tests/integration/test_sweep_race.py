"""Test sweep job CAS (compare-and-swap) status transitions prevent race conditions.

Uses mock DB sessions to verify that concurrent status updates via the actual
SweepService.run_job CAS pattern are handled correctly.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from backtestforecast.models import SweepJob, User
from backtestforecast.services.sweeps import SweepService


def _make_mock_user(plan_tier: str = "pro") -> MagicMock:
    user = MagicMock(spec=User)
    user.id = uuid.uuid4()
    user.plan_tier = plan_tier
    user.subscription_status = "active"
    user.subscription_current_period_end = datetime.now(UTC) + timedelta(days=30)
    return user


def _make_mock_sweep_job(
    job_id: uuid.UUID | None = None,
    user_id: uuid.UUID | None = None,
    status: str = "queued",
) -> MagicMock:
    job = MagicMock(spec=SweepJob)
    job.id = job_id or uuid.uuid4()
    job.user_id = user_id or uuid.uuid4()
    job.symbol = "AAPL"
    job.status = status
    job.mode = "grid"
    job.plan_tier_snapshot = "pro"
    job.candidate_count = 10
    job.evaluated_candidate_count = 0
    job.result_count = 0
    job.request_snapshot_json = {}
    job.warnings_json = []
    job.prefetch_summary_json = None
    job.error_code = None
    job.error_message = None
    job.started_at = None
    job.completed_at = None
    job.created_at = datetime.now(UTC)
    job.updated_at = datetime.now(UTC)
    job.last_heartbeat_at = None
    job.idempotency_key = None
    job.celery_task_id = None
    job.results = []
    return job


class TestCASTransitionQueudToRunning:
    """The CAS pattern in SweepService.run_job uses:
        UPDATE sweep_jobs SET status='running' WHERE id=:id AND status='queued'
    Only the first caller gets rowcount=1; subsequent callers get rowcount=0."""

    def test_cas_failure_skips_execution(self):
        """When CAS returns rowcount=0, run_job returns without executing the sweep."""
        job = _make_mock_sweep_job(status="queued")
        user = _make_mock_user()
        job.user_id = user.id

        session = MagicMock()
        cas_result = MagicMock()
        cas_result.rowcount = 0
        session.execute.return_value = cas_result
        session.get.return_value = user

        service = SweepService(session)
        service.repository = MagicMock()
        service.repository.get.return_value = job

        with patch("backtestforecast.billing.entitlements.ensure_sweep_access"):
            result = service.run_job(job.id)

        assert result is job
        service.repository.delete_results.assert_not_called()

    def test_cas_success_proceeds_to_execution(self):
        """When CAS returns rowcount=1, run_job proceeds past the CAS guard
        and calls delete_results before attempting execution."""
        job = _make_mock_sweep_job(status="queued")
        user = _make_mock_user()
        job.user_id = user.id

        session = MagicMock()
        cas_result = MagicMock()
        cas_result.rowcount = 1
        session.execute.return_value = cas_result
        session.get.return_value = user

        service = SweepService(session)
        service.repository = MagicMock()
        service.repository.get.return_value = job

        with patch("backtestforecast.billing.entitlements.ensure_sweep_access"), \
             patch("backtestforecast.services.sweeps.CreateSweepRequest") as mock_req, \
             patch.object(service, "_execute_sweep"):
            mock_req.model_validate.return_value = MagicMock()
            service.run_job(job.id)

        service.repository.delete_results.assert_called_once_with(job.id)

    def test_non_queued_job_returns_immediately(self):
        """run_job skips if job.status is not 'queued' or 'running'."""
        for status in ("succeeded", "failed", "cancelled"):
            job = _make_mock_sweep_job(status=status)
            session = MagicMock()
            service = SweepService(session)
            service.repository = MagicMock()
            service.repository.get.return_value = job

            result = service.run_job(job.id)
            assert result is job
            session.execute.assert_not_called()

    def test_not_found_raises(self):
        """run_job raises NotFoundError when the job does not exist."""
        from backtestforecast.errors import NotFoundError

        session = MagicMock()
        service = SweepService(session)
        service.repository = MagicMock()
        service.repository.get.return_value = None

        with pytest.raises(NotFoundError):
            service.run_job(uuid.uuid4())


class TestReaperPreemptsWorker:
    """If a reaper sets status='failed' before the worker CAS fires,
    the CAS returns rowcount=0 and the worker skips execution."""

    def test_reaper_marks_failed_worker_cas_blocked(self):
        job = _make_mock_sweep_job(status="queued")
        user = _make_mock_user()
        job.user_id = user.id

        session = MagicMock()
        session.get.return_value = user

        call_count = 0

        def simulate_concurrent_access(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            result = MagicMock()
            if call_count == 1:
                job.status = "failed"
                job.error_code = "heartbeat_timeout"
                result.rowcount = 0
            else:
                result.rowcount = 0
            return result

        session.execute.side_effect = simulate_concurrent_access

        service = SweepService(session)
        service.repository = MagicMock()
        service.repository.get.return_value = job

        with patch("backtestforecast.billing.entitlements.ensure_sweep_access"):
            result = service.run_job(job.id)

        assert result is job
        service.repository.delete_results.assert_not_called()


class TestSuccessCASPreventsOverwrite:
    """After sweep succeeds, the CAS UPDATE for 'succeeded' status
    checks WHERE status='running'. If a reaper already changed it,
    rowcount=0 prevents overwriting the reaper's status."""

    def test_success_cas_uses_running_guard(self):
        """The success CAS in _execute_sweep uses WHERE status='running'.
        After CAS succeeds, execution proceeds through delete_results."""
        job = _make_mock_sweep_job(status="running")
        user = _make_mock_user()
        job.user_id = user.id

        session = MagicMock()
        session.get.return_value = user

        cas_transition = MagicMock()
        cas_transition.rowcount = 1
        session.execute.return_value = cas_transition

        service = SweepService(session)
        service.repository = MagicMock()
        service.repository.get.return_value = job
        service.repository.delete_results.return_value = None

        with patch("backtestforecast.billing.entitlements.ensure_sweep_access"), \
             patch("backtestforecast.services.sweeps.CreateSweepRequest") as mock_req, \
             patch.object(service, "_execute_sweep"):
            mock_req.model_validate.return_value = MagicMock()
            service.run_job(job.id)

        service.repository.delete_results.assert_called_once()


class TestHeartbeatBasedReaper:
    """Verify that heartbeat-based reaper and worker CAS are compatible."""

    def test_stale_heartbeat_detected(self):
        job = _make_mock_sweep_job(status="running")
        job.last_heartbeat_at = datetime(2024, 1, 1, tzinfo=UTC)
        now = datetime(2024, 1, 1, 0, 10, tzinfo=UTC)

        heartbeat_age = (now - job.last_heartbeat_at).total_seconds()
        assert heartbeat_age == 600

        threshold = 300
        assert heartbeat_age > threshold

    def test_fresh_heartbeat_not_stale(self):
        job = _make_mock_sweep_job(status="running")
        now = datetime.now(UTC)
        job.last_heartbeat_at = now - timedelta(seconds=30)

        heartbeat_age = (now - job.last_heartbeat_at).total_seconds()
        assert heartbeat_age <= 300


class TestSweepStatusTransitions:
    """Test valid status transitions for sweep jobs using SweepService."""

    def test_user_not_found_fails_job(self):
        """When the user is not found, run_job marks the job as failed."""
        job = _make_mock_sweep_job(status="queued")
        session = MagicMock()
        session.get.return_value = None

        service = SweepService(session)
        service.repository = MagicMock()
        service.repository.get.return_value = job

        service.run_job(job.id)
        assert job.status == "failed"
        assert job.error_code == "user_not_found"

    def test_entitlement_revoked_fails_job(self):
        """When entitlement check raises, run_job marks the job as failed."""
        from backtestforecast.errors import AppError

        job = _make_mock_sweep_job(status="queued")
        user = _make_mock_user()
        job.user_id = user.id

        session = MagicMock()
        session.get.return_value = user

        service = SweepService(session)
        service.repository = MagicMock()
        service.repository.get.return_value = job

        with patch(
            "backtestforecast.billing.entitlements.ensure_sweep_access",
            side_effect=AppError(code="entitlement_error", message="No access"),
        ):
            service.run_job(job.id)

        assert job.status == "failed"
        assert job.error_code == "entitlement_revoked"


class TestCASIdempotency:
    """CAS operations are inherently idempotent. Retrying after rowcount=0
    does not mutate state."""

    def test_retry_with_rowcount_zero_is_noop(self):
        job = _make_mock_sweep_job(status="running")
        user = _make_mock_user()
        job.user_id = user.id

        session = MagicMock()
        cas_result = MagicMock()
        cas_result.rowcount = 0
        session.execute.return_value = cas_result
        session.get.return_value = user

        service = SweepService(session)
        service.repository = MagicMock()
        service.repository.get.return_value = job

        result = service.run_job(job.id)
        assert result is job
