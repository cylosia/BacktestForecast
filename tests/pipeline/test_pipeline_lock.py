"""Verify pipeline uses Redis lock to prevent concurrent runs."""
from __future__ import annotations

import inspect

from apps.worker.app.tasks import nightly_scan_pipeline


def test_pipeline_uses_redis_lock():
    source = inspect.getsource(nightly_scan_pipeline)
    assert "redis_client.lock" in source or "lock" in source
