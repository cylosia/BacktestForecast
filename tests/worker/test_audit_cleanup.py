"""Verify audit cleanup task structure."""
from __future__ import annotations

import inspect

from apps.worker.app.tasks import cleanup_audit_events


def test_cleanup_uses_batch_delete():
    """cleanup_audit_events should delete in batches to avoid table locks."""
    source = inspect.getsource(cleanup_audit_events)
    assert "BATCH_SIZE" in source
    assert "limit" in source.lower()


def test_cleanup_targets_high_volume_types():
    source = inspect.getsource(cleanup_audit_events)
    assert "export.downloaded" in source
    assert "backtest.viewed" in source
