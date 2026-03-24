"""Verify reaper uses skip_locked to avoid contention."""
from __future__ import annotations


def test_reaper_uses_skip_locked():
    import inspect

    from apps.worker.app.tasks import _reap_queued_jobs
    source = inspect.getsource(_reap_queued_jobs)
    assert "skip_locked" in source
