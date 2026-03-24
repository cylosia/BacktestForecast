"""Test that the pipeline task always releases its Redis lock in the finally block.

Regression test for the bug where the finally block guarded lock release
with 'if not _retrying', causing the lock to stay held when retry was
in flight and the explicit pre-retry release failed.
"""
from __future__ import annotations

import inspect


def test_finally_releases_lock_unconditionally():
    """The pipeline task finally block must call lock.release() without guards."""
    from apps.worker.app import tasks
    source = inspect.getsource(tasks.nightly_scan_pipeline)

    assert "finally:" in source, "nightly_scan_pipeline must have a finally block"
    assert "lock.release()" in source, "The task must call lock.release()"
    assert "redis_client.close()" in source, "The task must also close the Redis client"
    assert "_retrying" not in source, (
        "_retrying guards should not be present around lock cleanup"
    )


def test_no_retrying_variable():
    """The _retrying variable should be removed since it's no longer needed."""
    from apps.worker.app import tasks
    source = inspect.getsource(tasks.nightly_scan_pipeline)
    assert "_retrying" not in source, (
        "_retrying variable is no longer needed and should be removed"
    )
