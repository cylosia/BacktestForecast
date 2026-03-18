"""Shared Celery dispatch-and-handle-failure logic for create endpoints.

TODO: Implement transactional outbox pattern. The current approach commits
the DB record first, then sends the Celery task. A proper outbox would:
1. Write an OutboxMessage row in the same transaction as the job record
2. A separate poller would read pending OutboxMessages and send tasks
3. This eliminates the window where commit succeeds but task send fails
"""
from __future__ import annotations

import enum
from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import uuid4

import structlog
from sqlalchemy.orm import Session

from apps.worker.app.celery_app import celery_app
from backtestforecast.models import JobStatus


class DispatchResult(enum.Enum):
    """Outcome of a dispatch attempt."""
    SENT = "sent"
    SKIPPED = "skipped"
    PRE_COMMIT_FAILED = "pre_commit_failed"
    ENQUEUE_FAILED = "enqueue_failed"


@runtime_checkable
class Dispatchable(Protocol):
    status: str
    celery_task_id: str | None
    error_code: str | None
    error_message: str | None
    completed_at: datetime | None


def dispatch_celery_task(
    *,
    db: Session,
    job: Dispatchable,
    task_name: str,
    task_kwargs: dict[str, str],
    queue: str,
    log_event: str,
    logger: structlog.stdlib.BoundLogger,
    request_id: str | None = None,
    traceparent: str | None = None,
) -> DispatchResult:
    """Dispatch a Celery task for a newly created job.

    Commits the ``celery_task_id`` to the database **before** sending the
    task to the broker, so the worker never processes a job whose state
    has not been persisted.  If the subsequent task send fails, the job
    is marked ``"failed"`` with ``error_code="enqueue_failed"``.

    Returns a :class:`DispatchResult` indicating the outcome.
    """
    if job.status != JobStatus.QUEUED or job.celery_task_id is not None:
        logger.info(
            "dispatch.dispatch_skipped",
            log_event=log_event,
            reason="idempotent_return",
            status=job.status,
            has_celery_task_id=job.celery_task_id is not None,
            **task_kwargs,
        )
        return DispatchResult.SKIPPED

    headers: dict[str, str] = {}
    if request_id:
        headers["request_id"] = request_id
    if traceparent:
        headers["traceparent"] = traceparent

    # Commit-first pattern: persist celery_task_id before sending the task so
    # the worker never processes a job whose ID isn't in the DB. The tradeoff
    # is a brief window where the job is committed but the task hasn't been
    # sent yet (stuck-job). The reaper/stale-job recovery process handles this
    # by marking jobs that stay "queued" with a celery_task_id as failed.
    from backtestforecast.models import OutboxMessage
    task_id = str(uuid4())
    job.celery_task_id = task_id
    outbox_msg = OutboxMessage(
        task_name=task_name,
        task_kwargs_json=task_kwargs,
        queue=queue,
        status="pending",
        correlation_id=getattr(job, "id", None),
    )
    db.add(outbox_msg)
    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception(f"{log_event}.pre_commit_failed", **task_kwargs)
        job.status = JobStatus.FAILED
        job.error_code = "enqueue_failed"
        job.error_message = "Unable to persist task state before dispatch."
        try:
            db.commit()
        except Exception:
            db.rollback()
        return DispatchResult.PRE_COMMIT_FAILED

    try:
        celery_app.send_task(
            task_name, kwargs=task_kwargs, queue=queue,
            headers=headers if headers else None,
            task_id=task_id,
        )
    except Exception:
        logger.exception(
            "dispatch.enqueue_failed",
            log_event=log_event,
            celery_task_id=task_id,
            msg="Outbox message left pending for poller recovery.",
            **task_kwargs,
        )
        return DispatchResult.ENQUEUE_FAILED

    try:
        outbox_msg.status = "sent"
        db.commit()
    except Exception:
        db.rollback()

    logger.info("dispatch.enqueued", log_event=log_event, celery_task_id=task_id, **task_kwargs)
    return DispatchResult.SENT
