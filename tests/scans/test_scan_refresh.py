"""Verify scan refresh job creation uses nested transaction."""
from __future__ import annotations


def test_refresh_uses_nested_transaction():
    import inspect

    from backtestforecast.services.scans import ScanService
    source = inspect.getsource(ScanService.create_scheduled_refresh_jobs)
    assert "begin_nested" in source
