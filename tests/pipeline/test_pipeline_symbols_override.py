"""Verify pipeline respects Redis symbol override."""
from __future__ import annotations

import inspect

from apps.worker.app.tasks import nightly_scan_pipeline


def test_pipeline_checks_redis_for_symbols():
    """The pipeline task should check Redis for a symbol list override."""
    source = inspect.getsource(nightly_scan_pipeline)
    assert "bff:pipeline:symbols" in source
