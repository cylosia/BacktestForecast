"""Verify concurrent sweep limit is enforced by the sweep service."""
from __future__ import annotations

import inspect


def test_sweep_service_enforces_concurrent_limit():
    """Sweep quota enforcement must check max_concurrent_sweeps."""
    from backtestforecast.services.sweeps import SweepService

    source = inspect.getsource(SweepService._enforce_sweep_quota)
    assert "max_concurrent_sweeps" in source, (
        "Sweep quota enforcement must check active sweep count against max_concurrent_sweeps"
    )
    assert "QuotaExceededError" in source, (
        "Exceeding concurrent sweep limit must raise QuotaExceededError"
    )


def test_sweep_concurrent_check_queries_active_statuses():
    """Concurrent sweep check must count queued and running jobs."""
    from backtestforecast.services.sweeps import SweepService

    source = inspect.getsource(SweepService._enforce_sweep_quota)
    assert "queued" in source and "running" in source, (
        "Concurrent sweep check must count both queued and running jobs"
    )
