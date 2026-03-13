"""Shared Celery dispatch-and-handle-failure logic for create endpoints."""
from __future__ import annotations

from typing import Any

import structlog
from kombu.exceptions import KombuError, OperationalError as KombuOperationalError
from sqlalchemy.orm import Session

from apps.worker.app.celery_app import celery_app


def dispatch_celery_task(
    *,
    db: Session,
    job: Any,
    task_name: str,
    task_kwargs: dict[str, str],
    queue: str,
    log_event: str,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Dispatch a Celery task for a newly created job.

    Skips dispatch if the job is not ``"queued"`` or already has a
    ``celery_task_id`` (idempotent return).  On send failure the job is
    marked ``"failed"`` with ``error_code="enqueue_failed"`` and the
    failure is persisted (with a rollback safety net).

    ``job`` must be an ORM model with ``status``, ``celery_task_id``,
    ``error_code``, and ``error_message`` attributes.
    """
    if job.status != "queued" or job.celery_task_id is not None:
        return

    try:
        result = celery_app.send_task(task_name, kwargs=task_kwargs, queue=queue)
        job.celery_task_id = result.id
        db.commit()
        logger.info(f"{log_event}.enqueued", celery_task_id=result.id, **task_kwargs)
    except (OSError, ConnectionError, KombuError, KombuOperationalError):
        logger.exception(f"{log_event}.enqueue_failed", **task_kwargs)
        job.status = "failed"
        job.error_code = "enqueue_failed"
        job.error_message = "Unable to dispatch job. Please try again."
        try:
            db.commit()
        except Exception:
            logger.exception(f"{log_event}.enqueue_failed.commit_error", **task_kwargs)
            db.rollback()
