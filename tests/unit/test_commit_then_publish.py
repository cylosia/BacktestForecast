"""Tests for _commit_then_publish - the missing function that caused NameError."""
from __future__ import annotations

import warnings
from unittest.mock import MagicMock, patch
from uuid import uuid4


def _import_commit_then_publish():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _commit_then_publish
    return _commit_then_publish


def test_commit_then_publish_commits_and_publishes():
    _commit_then_publish = _import_commit_then_publish()

    session = MagicMock()
    job_id = uuid4()
    with patch("apps.worker.app.task_helpers.publish_job_status") as mock_publish:
        _commit_then_publish(session, "backtest", job_id, "failed", metadata={"error_code": "quota_exceeded"})
    session.commit.assert_called_once()
    mock_publish.assert_called_once_with("backtest", job_id, "failed", metadata={"error_code": "quota_exceeded"})


def test_commit_then_publish_rollback_on_commit_failure():
    _commit_then_publish = _import_commit_then_publish()

    session = MagicMock()
    session.commit.side_effect = Exception("DB down")
    job_id = uuid4()
    with patch("apps.worker.app.task_helpers.publish_job_status") as mock_publish:
        _commit_then_publish(session, "export", job_id, "failed")
    session.rollback.assert_called_once()
    mock_publish.assert_not_called()


def test_commit_then_publish_tolerates_publish_failure():
    _commit_then_publish = _import_commit_then_publish()

    session = MagicMock()
    job_id = uuid4()
    with patch("apps.worker.app.task_helpers.publish_job_status", side_effect=Exception("Redis down")):
        _commit_then_publish(session, "analysis", job_id, "failed")
    session.commit.assert_called_once()
