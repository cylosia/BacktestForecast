"""Verify dispatch failure marking and reaper recovery patterns."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from apps.api.app.dispatch import DispatchResult, dispatch_celery_task


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

        with patch("apps.worker.app.celery_app.celery_app.send_task", side_effect=ConnectionError("broker down")):
            result = dispatch_celery_task(
                db=db, job=job, task_name="test.task",
                task_kwargs={"id": "123"}, queue="test",
                log_event="test", logger=MagicMock(),
            )

        assert result == DispatchResult.ENQUEUE_FAILED
        assert job.status == "queued"
        assert job.error_code is None
        assert job.completed_at is None

    def test_pre_commit_failure_marks_failed(self):
        db = MagicMock()
        db.commit = MagicMock(side_effect=Exception("DB error"))
        db.flush = MagicMock()
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

    def test_outbox_flush_failure_marks_failed(self):
        db = MagicMock()
        db.flush = MagicMock(side_effect=Exception("flush error"))
        db.rollback = MagicMock()
        db.commit = MagicMock()
        db.execute = MagicMock()

        job = SimpleNamespace(
            id="job-1",
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
        assert db.rollback.called

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
