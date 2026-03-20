"""Test that ScanService.get_recommendations caps the SQL limit.

An unbounded limit parameter could cause OOM or very slow queries
when a scanner job has thousands of recommendations.
"""
from __future__ import annotations

import inspect

from backtestforecast.services.scans import ScanService


def test_get_recommendations_caps_limit() -> None:
    """get_recommendations must cap the limit to prevent unbounded queries."""
    source = inspect.getsource(ScanService.get_recommendations)
    assert "effective_limit" in source and "min(limit" in source, (
        "ScanService.get_recommendations must cap the limit using min(limit, ...)"
    )
