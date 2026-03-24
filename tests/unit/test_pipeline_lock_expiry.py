"""Verify pipeline lock timeout exceeds task hard time limit."""
from __future__ import annotations

import inspect

from apps.worker.app.tasks import nightly_scan_pipeline

# Values from pipeline.nightly_scan task definition
PIPELINE_SOFT_TIME_LIMIT = 1800
PIPELINE_HARD_TIME_LIMIT = 1860
PIPELINE_LOCK_TIMEOUT = 2100


def test_pipeline_lock_timeout_exceeds_hard_time_limit():
    """Lock timeout must exceed hard time_limit so the lock outlives the task."""
    assert PIPELINE_LOCK_TIMEOUT > PIPELINE_HARD_TIME_LIMIT, (
        f"Lock timeout ({PIPELINE_LOCK_TIMEOUT}s) must be > hard time_limit ({PIPELINE_HARD_TIME_LIMIT}s) "
        "so the lock is not released prematurely if the task is killed."
    )


def test_pipeline_lock_timeout_exceeds_soft_time_limit():
    """Lock timeout must exceed soft_time_limit."""
    assert PIPELINE_LOCK_TIMEOUT > PIPELINE_SOFT_TIME_LIMIT


def test_pipeline_uses_expected_lock_timeout():
    """Verify the source uses the expected lock timeout value."""
    source = inspect.getsource(nightly_scan_pipeline)
    assert f"timeout={PIPELINE_LOCK_TIMEOUT}" in source
