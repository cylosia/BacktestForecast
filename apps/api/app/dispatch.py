"""Shared Celery dispatch-and-handle-failure logic for create endpoints."""
from __future__ import annotations

from typing import Any

import structlog
from kombu.exceptions import KombuError, OperationalError as KombuOperationalError
from prometheus_client import Counter
from sqlalchemy.orm import Session

from apps.worker.app.celery_app import celery_app

DISPATCH_REVOKE_FAILED = Counter(
    "dispatch_revoke_failed_total",
    "Times celery task revocation failed after a commit error during dispatch",
)


def dispatch_celery_task(
    *,
    db: Session,
    job: Any,
    task_name: str,
    task_kwargs: dict[str, str],
    queue: str,
    log_event: str,
    logger: structlog.stdlib.BoundLogger,
    request_id: str | None = None,
    traceparent: str | None = None,
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
        logger.info(
            f"{log_event}.dispatch_skipped",
            reason="idempotent_return",
            status=job.status,
            has_celery_task_id=job.celery_task_id is not None,
            **task_kwargs,
        )
        return

    headers: dict[str, str] = {}
    if request_id:
        headers["request_id"] = request_id
    if traceparent:
        headers["traceparent"] = traceparent

    try:
        result = celery_app.send_task(
            task_name, kwargs=task_kwargs, queue=queue,
            headers=headers if headers else None,
        )
        job.celery_task_id = result.id
        try:
            db.commit()
        except Exception:
            db.rollback()
            logger.exception(f"{log_event}.enqueued_but_commit_failed", celery_task_id=result.id, **task_kwargs)
            try:
                celery_app.control.revoke(result.id)
            except Exception:
                DISPATCH_REVOKE_FAILED.inc()
                logger.warning(f"{log_event}.revoke_failed", celery_task_id=result.id)
            raise
        logger.info(f"{log_event}.enqueued", celery_task_id=result.id, **task_kwargs)
    except (OSError, KombuError, KombuOperationalError, TimeoutError, ConnectionError, RuntimeError) as exc:
        logger.exception(f"{log_event}.enqueue_failed", **task_kwargs)
        job.status = "failed"
        job.error_code = "enqueue_failed"
        job.error_message = "Unable to dispatch job. Please try again."
        try:
            db.commit()
        except Exception:
            logger.exception(f"{log_event}.enqueue_failed.commit_error", **task_kwargs)
            db.rollback()
