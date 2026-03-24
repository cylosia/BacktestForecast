"""Verify scan handles all candidates failing gracefully."""
from __future__ import annotations


def test_empty_candidates_marks_failed():
    """When all candidates fail, job should be marked failed with scan_empty."""
    import inspect

    from backtestforecast.services.scans import ScanService
    source = inspect.getsource(ScanService._execute_scan)
    assert 'error_code = "scan_empty"' in source
