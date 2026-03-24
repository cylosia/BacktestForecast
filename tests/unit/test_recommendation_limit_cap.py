"""Test that the scanner recommendations query caps the SQL limit.

An unbounded limit parameter could cause OOM or very slow queries
when a scanner job has thousands of recommendations.
"""
from __future__ import annotations

import inspect

import backtestforecast.services.scans as scans_module


def test_get_recommendations_caps_limit() -> None:
    """get_recommendations must cap the limit to prevent unbounded queries."""
    source = inspect.getsource(scans_module)
    assert "effective_limit = min(limit, 200)" in source, (
        "Scanner recommendation retrieval must cap the limit using min(limit, 200)"
    )
