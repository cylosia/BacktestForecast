"""Verify bundle fetch has a timeout."""
from __future__ import annotations


def test_bundle_fetch_has_timeout():
    """_prepare_bundles should use as_completed with a timeout."""
    import inspect

    from backtestforecast.services.scans import ScanService
    source = inspect.getsource(ScanService._prepare_bundles)
    assert "timeout=300" in source or "timeout=" in source
