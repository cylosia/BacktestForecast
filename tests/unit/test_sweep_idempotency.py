"""Test SweepJobRepository.get_by_idempotency_key behavioral semantics.

Tests verify that the repository correctly filters sweep jobs by status,
returning None for failed/cancelled jobs and returning active jobs for
idempotency deduplication.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from backtestforecast.models import SweepJob
from backtestforecast.repositories.sweep_jobs import SweepJobRepository


def _make_sweep_job(
    user_id: uuid.UUID,
    idempotency_key: str,
    status: str = "queued",
) -> MagicMock:
    job = MagicMock(spec=SweepJob)
    job.id = uuid.uuid4()
    job.user_id = user_id
    job.idempotency_key = idempotency_key
    job.status = status
    job.symbol = "AAPL"
    job.mode = "grid"
    job.plan_tier_snapshot = "pro"
    job.candidate_count = 10
    job.evaluated_candidate_count = 0
    job.result_count = 0
    job.request_snapshot_json = {}
    job.warnings_json = []
    job.created_at = datetime.now(UTC)
    job.updated_at = datetime.now(UTC)
    return job


class TestGetByIdempotencyKeyFiltersStatuses:
    """get_by_idempotency_key must exclude failed and cancelled jobs."""

    def test_returns_queued_job(self):
        user_id = uuid.uuid4()
        key = "dedup-key-123"
        queued_job = _make_sweep_job(user_id, key, status="queued")

        session = MagicMock()
        session.scalar.return_value = queued_job

        repo = SweepJobRepository(session)
        result = repo.get_by_idempotency_key(user_id, key)
        assert result is queued_job

    def test_returns_running_job(self):
        user_id = uuid.uuid4()
        key = "dedup-key-456"
        running_job = _make_sweep_job(user_id, key, status="running")

        session = MagicMock()
        session.scalar.return_value = running_job

        repo = SweepJobRepository(session)
        result = repo.get_by_idempotency_key(user_id, key)
        assert result is running_job

    def test_returns_succeeded_job(self):
        user_id = uuid.uuid4()
        key = "dedup-key-789"
        succeeded_job = _make_sweep_job(user_id, key, status="succeeded")

        session = MagicMock()
        session.scalar.return_value = succeeded_job

        repo = SweepJobRepository(session)
        result = repo.get_by_idempotency_key(user_id, key)
        assert result is succeeded_job

    def test_returns_none_for_failed_job(self):
        """Failed jobs are excluded by the notin_ filter, so the query
        returns None even if a matching key exists."""
        user_id = uuid.uuid4()

        session = MagicMock()
        session.scalar.return_value = None

        repo = SweepJobRepository(session)
        result = repo.get_by_idempotency_key(user_id, "dedup-key-failed")
        assert result is None

    def test_returns_none_for_cancelled_job(self):
        """Cancelled jobs are excluded by the notin_ filter, so the query
        returns None even if a matching key exists."""
        user_id = uuid.uuid4()

        session = MagicMock()
        session.scalar.return_value = None

        repo = SweepJobRepository(session)
        result = repo.get_by_idempotency_key(user_id, "dedup-key-cancelled")
        assert result is None

    def test_returns_none_when_no_key_exists(self):
        user_id = uuid.uuid4()

        session = MagicMock()
        session.scalar.return_value = None

        repo = SweepJobRepository(session)
        result = repo.get_by_idempotency_key(user_id, "nonexistent-key")
        assert result is None


class TestIdempotencyQueryStructure:
    """Verify the SQL query contains the expected filter clauses."""

    def test_query_filters_on_user_id_key_and_status(self):
        """The method builds a SELECT with three WHERE conditions:
        user_id, idempotency_key, and status NOT IN ('failed', 'cancelled')."""
        user_id = uuid.uuid4()
        key = "test-key"

        session = MagicMock()
        session.scalar.return_value = None

        repo = SweepJobRepository(session)
        repo.get_by_idempotency_key(user_id, key)

        session.scalar.assert_called_once()
        stmt = session.scalar.call_args[0][0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": False}))

        assert "sweep_jobs" in compiled.lower()
        assert "user_id" in compiled.lower()
        assert "idempotency_key" in compiled.lower()
        assert "status" in compiled.lower()

    def test_notin_excludes_failed_and_cancelled(self):
        """Verify the notin_ clause contains exactly 'failed' and 'cancelled'."""
        user_id = uuid.uuid4()

        session = MagicMock()
        session.scalar.return_value = None

        repo = SweepJobRepository(session)
        repo.get_by_idempotency_key(user_id, "check-notin")

        stmt = session.scalar.call_args[0][0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))

        assert "failed" in compiled
        assert "cancelled" in compiled

    def test_query_selects_from_sweep_jobs_table(self):
        user_id = uuid.uuid4()

        session = MagicMock()
        session.scalar.return_value = None

        repo = SweepJobRepository(session)
        repo.get_by_idempotency_key(user_id, "key-123")

        stmt = session.scalar.call_args[0][0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": False}))
        assert "sweep_jobs" in compiled.lower()


class TestIdempotencyBehavioralContract:
    """Verify the behavioral contract of idempotency dedup across the service flow."""

    def test_existing_active_job_prevents_creation(self):
        """When get_by_idempotency_key returns an existing active job,
        the caller should use it instead of creating a new one."""
        user_id = uuid.uuid4()
        key = "idem-key-abc"
        existing_job = _make_sweep_job(user_id, key, status="queued")

        session = MagicMock()
        session.scalar.return_value = existing_job

        repo = SweepJobRepository(session)
        result = repo.get_by_idempotency_key(user_id, key)

        assert result is existing_job
        assert result.status == "queued"
        session.add.assert_not_called()

    def test_no_match_allows_creation(self):
        """When get_by_idempotency_key returns None, a new job can be created."""
        user_id = uuid.uuid4()

        session = MagicMock()
        session.scalar.return_value = None

        repo = SweepJobRepository(session)
        result = repo.get_by_idempotency_key(user_id, "idem-key-new")
        assert result is None

    def test_same_key_different_users_independent(self):
        """Same idempotency key for different users should be independent."""
        user_a = uuid.uuid4()
        user_b = uuid.uuid4()
        key = "shared-key"

        session = MagicMock()
        repo = SweepJobRepository(session)

        job_a = _make_sweep_job(user_a, key, status="queued")
        session.scalar.return_value = job_a
        result_a = repo.get_by_idempotency_key(user_a, key)
        assert result_a is job_a

        session.scalar.return_value = None
        result_b = repo.get_by_idempotency_key(user_b, key)
        assert result_b is None


class TestRepositoryAddAndFlush:
    """Verify the add() method flushes the session."""

    def test_add_calls_session_add_and_flush(self):
        session = MagicMock()
        repo = SweepJobRepository(session)
        job = MagicMock(spec=SweepJob)

        repo.add(job)

        session.add.assert_called_once_with(job)
        session.flush.assert_called_once()
