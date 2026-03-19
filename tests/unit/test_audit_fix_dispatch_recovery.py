"""Verify dispatch failure marking and reaper recovery patterns."""
from __future__ import annotations
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from datetime import UTC, datetime

from apps.api.app.dispatch import dispatch_celery_task, DispatchResult


class TestDispatchCrashWindow:
    def test_enqueue_failure_marks_job_failed(self):
        db = MagicMock()
        db.commit = MagicMock(side_effect=[None, None])  # pre-commit ok, then post-fail ok

        job = SimpleNamespace(
            status="queued",
            celery_task_id=None,
            error_code=None,
            error_message=None,
            completed_at=None,
        )

        with patch("apps.api.app.dispatch.celery_app") as mock_celery:
            mock_celery.send_task = MagicMock(side_effect=ConnectionError("broker down"))
            result = dispatch_celery_task(
                db=db, job=job, task_name="test.task",
                task_kwargs={"id": "123"}, queue="test",
                log_event="test", logger=MagicMock(),
            )

        assert result == DispatchResult.ENQUEUE_FAILED
        assert job.status == "failed"
        assert job.error_code == "enqueue_failed"
        assert job.completed_at is not None

    def test_pre_commit_failure_marks_failed(self):
        db = MagicMock()
        db.commit = MagicMock(side_effect=Exception("DB error"))
        db.rollback = MagicMock()

        job = SimpleNamespace(
            status="queued",
            celery_task_id=None,
            error_code=None,
            error_message=None,
            completed_at=None,
        )

        result = dispatch_celery_task(
            db=db, job=job, task_name="test.task",
            task_kwargs={"id": "123"}, queue="test",
            log_event="test", logger=MagicMock(),
        )

        assert result == DispatchResult.PRE_COMMIT_FAILED

    def test_skips_non_queued_jobs(self):
        db = MagicMock()
        job = SimpleNamespace(
            status="running",
            celery_task_id="existing-id",
            error_code=None,
            error_message=None,
            completed_at=None,
        )

        result = dispatch_celery_task(
            db=db, job=job, task_name="test.task",
            task_kwargs={"id": "123"}, queue="test",
            log_event="test", logger=MagicMock(),
        )

        assert result == DispatchResult.SKIPPED
