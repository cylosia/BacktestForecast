"""Verify S3 reconciliation uses streaming iteration, not list().

Audit fix 18: Loading all S3 keys into memory causes OOM for large buckets.
The reconcile task must use iter_keys() instead of list_keys().
"""
from __future__ import annotations

import inspect


def test_reconcile_uses_iter_keys():
    """reconcile_s3_orphans must call iter_keys(), not list_keys()."""
    from apps.worker.app.tasks import reconcile_s3_orphans

    source = inspect.getsource(reconcile_s3_orphans)
    assert "iter_keys" in source, (
        "reconcile_s3_orphans must use iter_keys() for streaming S3 key enumeration"
    )
    assert "list_keys" not in source, (
        "reconcile_s3_orphans must NOT use list_keys() which loads all keys into memory"
    )


def test_s3_storage_has_iter_keys():
    """S3Storage must expose iter_keys as a generator method."""
    from backtestforecast.exports.storage import S3Storage

    assert hasattr(S3Storage, "iter_keys"), "S3Storage.iter_keys not found"
    assert hasattr(S3Storage, "list_keys"), "S3Storage.list_keys not found"


def test_iter_keys_is_generator():
    """iter_keys must be a generator function, not one that returns a list."""
    import inspect

    from backtestforecast.exports.storage import S3Storage

    assert inspect.isgeneratorfunction(S3Storage.iter_keys), (
        "iter_keys must be a generator function"
    )
