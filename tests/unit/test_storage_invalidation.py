"""Verify export storage invalidation is registered."""
from __future__ import annotations

from backtestforecast.config import _invalidation_callbacks


def test_storage_invalidation_registered():
    """_invalidate_storage must be in the invalidation callback list."""
    callback_names = [cb.__name__ for cb in _invalidation_callbacks]
    assert "_invalidate_storage" in callback_names, (
        "_invalidate_storage is not registered; S3 credential rotation won't take effect"
    )
