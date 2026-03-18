"""Tests for S3Storage public API methods (audit item C-4 / items 20-23)."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backtestforecast.exports.storage import S3Storage


def _make_storage(bucket="test-bucket", prefix="exports/"):
    """Create an S3Storage without calling __init__ (avoids boto3 dependency)."""
    storage = S3Storage.__new__(S3Storage)
    storage._bucket = bucket
    storage._prefix = prefix
    storage._client = MagicMock()
    return storage


def test_bucket_property():
    storage = _make_storage()
    assert storage.bucket == "test-bucket"


def test_prefix_property():
    storage = _make_storage()
    assert storage.prefix == "exports/"


def test_client_property():
    storage = _make_storage()
    assert storage.client is not None


def test_list_keys():
    storage = _make_storage()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "exports/a.csv"}, {"Key": "exports/b.csv"}]},
        {"Contents": [{"Key": "exports/c.csv"}]},
    ]
    storage.client.get_paginator.return_value = paginator

    keys = storage.list_keys()
    assert keys == ["exports/a.csv", "exports/b.csv", "exports/c.csv"]


def test_list_keys_empty():
    storage = _make_storage()
    paginator = MagicMock()
    paginator.paginate.return_value = [{}]
    storage.client.get_paginator.return_value = paginator

    keys = storage.list_keys()
    assert keys == []
