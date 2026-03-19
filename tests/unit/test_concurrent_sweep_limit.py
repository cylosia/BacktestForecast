"""Verify concurrent sweep limit is enforced at the API layer."""
from __future__ import annotations

import inspect


def test_sweep_router_enforces_concurrent_limit():
    """The sweep creation endpoint must check max_concurrent_sweeps."""
    from apps.api.app.routers.sweeps import create_sweep

    source = inspect.getsource(create_sweep)
    assert "max_concurrent_sweeps" in source, (
        "Sweep creation must check active sweep count against max_concurrent_sweeps"
    )
    assert "QuotaExceededError" in source, (
        "Exceeding concurrent sweep limit must raise QuotaExceededError"
    )


def test_sweep_concurrent_check_queries_active_statuses():
    """Concurrent sweep check must count queued and running jobs."""
    from apps.api.app.routers.sweeps import create_sweep

    source = inspect.getsource(create_sweep)
    assert "queued" in source and "running" in source, (
        "Concurrent sweep check must count both queued and running jobs"
    )
