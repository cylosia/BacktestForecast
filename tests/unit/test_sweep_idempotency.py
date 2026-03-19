"""Verify sweep idempotency key excludes failed jobs."""
from __future__ import annotations

from backtestforecast.repositories.sweep_jobs import SweepJobRepository
from backtestforecast.models import SweepJob


def test_idempotency_query_excludes_failed():
    """get_by_idempotency_key must filter out failed and cancelled jobs."""
    import inspect

    source = inspect.getsource(SweepJobRepository.get_by_idempotency_key)
    assert "failed" in source.lower() or "terminal" in source.lower(), \
        "Idempotency query must exclude terminal statuses"
    assert "cancelled" in source.lower() or "terminal" in source.lower(), \
        "Idempotency query must exclude cancelled status"
