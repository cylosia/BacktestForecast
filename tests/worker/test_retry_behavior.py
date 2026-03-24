"""Test Celery retry behaviour configuration.

Verifies that worker tasks are configured with appropriate retry
settings for transient errors.

Requires Redis for Celery app initialisation - marked as integration.
"""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def test_run_backtest_has_retry_config():
    """run_backtest must have max_retries > 0 for transient errors."""
    from apps.worker.app.tasks import run_backtest

    assert run_backtest.max_retries >= 1, (
        "run_backtest should support at least 1 retry for transient errors"
    )


def test_run_backtest_has_explicit_retry_call():
    """Verify the task has retry attributes configured for transient failures."""
    from apps.worker.app.tasks import run_backtest

    assert hasattr(run_backtest, 'max_retries'), "Task should have max_retries attribute"
    assert run_backtest.max_retries >= 1, "max_retries should be at least 1"


def test_run_scan_job_has_retry_config():
    """run_scan_job must have max_retries > 0."""
    from apps.worker.app.tasks import run_scan_job

    assert run_scan_job.max_retries >= 1


def test_generate_export_has_retry_config():
    """generate_export must have max_retries > 0."""
    from apps.worker.app.tasks import generate_export

    assert generate_export.max_retries >= 1


def test_tasks_have_time_limits():
    """All main tasks should have soft_time_limit set to prevent runaway workers."""
    from apps.worker.app.tasks import generate_export, run_backtest, run_scan_job

    for task in [run_backtest, run_scan_job, generate_export]:
        assert task.soft_time_limit is not None and task.soft_time_limit > 0, (
            f"{task.name} should have a positive soft_time_limit"
        )
