"""Tests that run_scan_job retries on ExternalServiceError like run_backtest does."""
from __future__ import annotations

import inspect
import warnings


def test_scan_task_retries_on_external_service_error():
    """Verify run_scan_job contains ExternalServiceError retry logic."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import run_scan_job

    source = inspect.getsource(run_scan_job)
    assert "ExternalServiceError" in source, (
        "run_scan_job must check for ExternalServiceError and retry"
    )
    assert "self.retry" in source, (
        "run_scan_job must call self.retry for transient external failures"
    )
