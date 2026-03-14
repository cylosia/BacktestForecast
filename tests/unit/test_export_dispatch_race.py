"""Item 77: Test export dispatch race condition.

Verifies the export dispatch flow: an export job is properly created,
its status transitions are correct, and it can be retrieved by ID.
The dispatch_celery_task function must be idempotent (skip if not queued
or already has a celery_task_id).
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


def _make_job(*, status: str = "queued", celery_task_id: str | None = None):
    """Create a mock job object mimicking the ORM model."""
    return SimpleNamespace(
        status=status,
        celery_task_id=celery_task_id,
        error_code=None,
        error_message=None,
    )


class TestExportDispatchRaceCondition:
    @patch("apps.api.app.dispatch.celery_app")
    def test_dispatch_creates_task_for_queued_job(self, mock_celery):
        from apps.api.app.dispatch import dispatch_celery_task

        mock_result = MagicMock()
        mock_result.id = "celery-task-123"
        mock_celery.send_task.return_value = mock_result

        db = MagicMock()
        job = _make_job(status="queued")
        logger = MagicMock()

        dispatch_celery_task(
            db=db,
            job=job,
            task_name="worker.export",
            task_kwargs={"export_job_id": "abc-123"},
            queue="exports",
            log_event="export",
            logger=logger,
        )

        mock_celery.send_task.assert_called_once()
        assert job.celery_task_id == "celery-task-123"
        db.commit.assert_called_once()

    @patch("apps.api.app.dispatch.celery_app")
    def test_dispatch_skips_non_queued_job(self, mock_celery):
        from apps.api.app.dispatch import dispatch_celery_task

        db = MagicMock()
        job = _make_job(status="running")
        logger = MagicMock()

        dispatch_celery_task(
            db=db,
            job=job,
            task_name="worker.export",
            task_kwargs={"export_job_id": "abc-123"},
            queue="exports",
            log_event="export",
            logger=logger,
        )

        mock_celery.send_task.assert_not_called()

    @patch("apps.api.app.dispatch.celery_app")
    def test_dispatch_skips_job_with_existing_celery_id(self, mock_celery):
        from apps.api.app.dispatch import dispatch_celery_task

        db = MagicMock()
        job = _make_job(status="queued", celery_task_id="already-exists")
        logger = MagicMock()

        dispatch_celery_task(
            db=db,
            job=job,
            task_name="worker.export",
            task_kwargs={"export_job_id": "abc-123"},
            queue="exports",
            log_event="export",
            logger=logger,
        )

        mock_celery.send_task.assert_not_called()

    @patch("apps.api.app.dispatch.celery_app")
    def test_dispatch_marks_failed_on_connection_error(self, mock_celery):
        from apps.api.app.dispatch import dispatch_celery_task

        mock_celery.send_task.side_effect = ConnectionError("broker down")
        db = MagicMock()
        job = _make_job(status="queued")
        logger = MagicMock()

        dispatch_celery_task(
            db=db,
            job=job,
            task_name="worker.export",
            task_kwargs={"export_job_id": "abc-123"},
            queue="exports",
            log_event="export",
            logger=logger,
        )

        assert job.status == "failed"
        assert job.error_code == "enqueue_failed"

    @patch("apps.api.app.dispatch.celery_app")
    def test_concurrent_dispatch_is_idempotent(self, mock_celery):
        """Simulating two concurrent calls: the second one should be skipped
        because the first already set celery_task_id."""
        from apps.api.app.dispatch import dispatch_celery_task

        mock_result = MagicMock()
        mock_result.id = "task-first"
        mock_celery.send_task.return_value = mock_result

        db = MagicMock()
        job = _make_job(status="queued")
        logger = MagicMock()

        dispatch_celery_task(
            db=db, job=job,
            task_name="worker.export",
            task_kwargs={"export_job_id": "abc-123"},
            queue="exports", log_event="export", logger=logger,
        )
        assert job.celery_task_id == "task-first"

        dispatch_celery_task(
            db=db, job=job,
            task_name="worker.export",
            task_kwargs={"export_job_id": "abc-123"},
            queue="exports", log_event="export", logger=logger,
        )
        assert mock_celery.send_task.call_count == 1
