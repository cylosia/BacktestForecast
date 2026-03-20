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

    finally_pos = source.rfind("finally:")
    assert finally_pos >= 0, "nightly_scan_pipeline must have a finally block"
    finally_block = source[finally_pos:]

    assert "lock.release()" in finally_block, (
        "The finally block must call lock.release()"
    )
    lock_line_start = finally_block.find("lock.release()")
    pre_lock = finally_block[:lock_line_start]
    assert "_retrying" not in pre_lock, (
        "lock.release() in the finally block must NOT be guarded by _retrying. "
        "The lock should always be released regardless of retry state."
    )


def test_no_retrying_variable():
    """The _retrying variable should be removed since it's no longer needed."""
    from apps.worker.app import tasks
    source = inspect.getsource(tasks.nightly_scan_pipeline)
    assert "_retrying" not in source, (
        "_retrying variable is no longer needed and should be removed"
    )
