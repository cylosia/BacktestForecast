"""Tests for SweepJobRepository correctness."""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from backtestforecast.repositories.sweep_jobs import SweepJobRepository


class TestSweepJobRepository:
    def test_init_stores_session(self):
        session = MagicMock()
        repo = SweepJobRepository(session)
        assert repo.session is session

    def test_add_flushes(self):
        session = MagicMock()
        repo = SweepJobRepository(session)
        job = MagicMock()
        result = repo.add(job)
        session.add.assert_called_once_with(job)
        session.flush.assert_called_once()
        assert result is job

    def test_get_by_idempotency_key(self):
        session = MagicMock()
        session.scalar.return_value = None
        repo = SweepJobRepository(session)
        result = repo.get_by_idempotency_key(uuid.uuid4(), "key-123")
        assert result is None
        session.scalar.assert_called_once()

    def test_count_for_user(self):
        session = MagicMock()
        session.scalar.return_value = 5
        repo = SweepJobRepository(session)
        assert repo.count_for_user(uuid.uuid4()) == 5

    def test_get_for_user_without_results(self):
        session = MagicMock()
        session.scalar.return_value = None
        repo = SweepJobRepository(session)
        result = repo.get_for_user(uuid.uuid4(), uuid.uuid4())
        assert result is None

    def test_get_with_for_update(self):
        session = MagicMock()
        session.scalar.return_value = MagicMock()
        repo = SweepJobRepository(session)
        result = repo.get(uuid.uuid4(), for_update=True)
        assert result is not None


def test_sweep_idempotency_excludes_failed_jobs():
    """Failed sweep jobs should not block retry with same idempotency key."""
    import inspect
    source = inspect.getsource(SweepJobRepository.get_by_idempotency_key)
    assert "notin_" in source, "Sweep idempotency must exclude failed/cancelled jobs"


def test_sweep_safe_validate_summary_handles_corrupt_data():
    """Corrupt summary JSON should not crash the sweep results endpoint."""
    from backtestforecast.services.sweeps import _safe_validate_summary
    result = _safe_validate_summary({"invalid_field": "bad_data"})
    assert result is not None
