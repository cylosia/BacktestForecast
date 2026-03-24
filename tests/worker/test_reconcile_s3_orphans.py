"""Tests for the reconcile_s3_orphans maintenance task."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.filterwarnings("ignore:MASSIVE_API_KEY:UserWarning")


def _make_session(scalars_return):
    """Build a MagicMock session that works as a context manager."""
    session = MagicMock()
    session.scalars.return_value = scalars_return
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    return session


@patch("apps.worker.app.tasks.create_worker_session")
def test_no_orphans_deletes_nothing(mock_create_session):
    """When every S3 key has a matching ExportJob record, nothing is deleted."""
    from apps.worker.app.tasks import reconcile_s3_orphans

    keys = ["exports/file1.csv", "exports/file2.csv"]
    session = _make_session(keys)
    mock_create_session.return_value = session

    mock_storage = MagicMock()
    mock_storage.iter_keys.return_value = iter(keys)

    mock_settings = MagicMock()
    mock_settings.s3_bucket = "my-bucket"

    with (
        patch("backtestforecast.config.get_settings", return_value=mock_settings),
        patch("backtestforecast.exports.storage.S3Storage", return_value=mock_storage),
    ):
        reconcile_s3_orphans()

    mock_storage.delete.assert_not_called()


@patch("apps.worker.app.tasks.create_worker_session")
def test_orphan_detected_and_deleted(mock_create_session):
    """An S3 key with no matching DB record must be deleted."""
    from apps.worker.app.tasks import reconcile_s3_orphans

    existing_key = "exports/file1.csv"
    orphan_key = "exports/orphan.csv"

    session = _make_session([existing_key])
    mock_create_session.return_value = session

    mock_storage = MagicMock()
    mock_storage.iter_keys.return_value = iter([existing_key, orphan_key])

    mock_settings = MagicMock()
    mock_settings.s3_bucket = "my-bucket"

    with (
        patch("backtestforecast.config.get_settings", return_value=mock_settings),
        patch("backtestforecast.exports.storage.S3Storage", return_value=mock_storage),
    ):
        reconcile_s3_orphans()

    mock_storage.delete.assert_called_once_with(orphan_key)


def test_s3_disabled_returns_early():
    """When s3_bucket is not configured, the task returns immediately."""
    from apps.worker.app.tasks import reconcile_s3_orphans

    mock_settings = MagicMock()
    mock_settings.s3_bucket = ""

    with patch("backtestforecast.config.get_settings", return_value=mock_settings):
        reconcile_s3_orphans()
