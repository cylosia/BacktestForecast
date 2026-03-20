"""Shared Celery dispatch-and-handle-failure logic for create endpoints.

Uses a transactional outbox pattern: an OutboxMessage row is written in the
same DB transaction as the job record.  After commit, the task is sent to
Celery optimistically.  If the send succeeds the outbox row is marked
"sent"; if it fails the row stays "pending" and the ``poll_outbox`` Celery
beat task will pick it up within 60 seconds.
"""
from __future__ import annotations

import enum
import time
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable
from uuid import uuid4

import structlog
from sqlalchemy.orm import Session

from backtestforecast.schemas.common import RunJobStatus
from backtestforecast.observability.metrics import DISPATCH_RESULTS_TOTAL

_SEND_MAX_ATTEMPTS = 1
_SEND_RETRY_DELAY = 0.0


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

    Writes both the ``celery_task_id`` and an ``OutboxMessage`` row in the
    same transaction, then commits.  After commit, attempts inline delivery
    to Celery with up to 2 retries.

    - On success: outbox row marked ``"sent"``, returns ``SENT``.
    - On failure with outbox committed: job stays ``"queued"``, outbox
      stays ``"pending"`` for the ``poll_outbox`` beat task to recover.
    - On failure without outbox: job marked ``"failed"`` immediately.

    Returns a :class:`DispatchResult` indicating the outcome.
    """
    for k, v in list(task_kwargs.items()):
        if not isinstance(v, str):
            logger.warning("dispatch.non_string_kwarg", key=k, type=type(v).__name__)
            task_kwargs[k] = str(v)

    if job.status != RunJobStatus.QUEUED or job.celery_task_id is not None:
        logger.info(
            "dispatch.dispatch_skipped",
            log_event=log_event,
            reason="idempotent_return",
            status=job.status,
            has_celery_task_id=job.celery_task_id is not None,
            **task_kwargs,
        )
        DISPATCH_RESULTS_TOTAL.labels(result=DispatchResult.SKIPPED.value, task_name=task_name).inc()
        return DispatchResult.SKIPPED

    headers: dict[str, str] = {}
    if request_id:
        headers["request_id"] = request_id
    if traceparent:
        headers["traceparent"] = traceparent

    task_id = str(uuid4())
    job.celery_task_id = task_id

    # Write an OutboxMessage in the same transaction as the job update so
    # that if the process crashes after commit but before send_task, the
    # poll_outbox beat task will pick up the pending message.
    outbox_msg = None
    try:
        from backtestforecast.models import OutboxMessage
        job_id = getattr(job, "id", None)
        outbox_msg = OutboxMessage(
            task_name=task_name,
            task_kwargs_json=task_kwargs,
            queue=queue,
            status="pending",
            correlation_id=job_id,
        )
        db.add(outbox_msg)
    except Exception:
        logger.warning("dispatch.outbox_write_skipped", exc_info=True)

    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception(f"{log_event}.pre_commit_failed", **task_kwargs)
        try:
            from sqlalchemy import update as _sa_update
            job_id = getattr(job, "id", None)
            if job_id is not None:
                model_cls = type(job)
                db.execute(
                    _sa_update(model_cls)
                    .where(model_cls.id == job_id)
                    .values(
                        status=RunJobStatus.FAILED,
                        error_code="enqueue_failed",
                        error_message="Unable to persist task state before dispatch.",
                    )
                )
                db.commit()
        except Exception:
            db.rollback()
        DISPATCH_RESULTS_TOTAL.labels(result=DispatchResult.PRE_COMMIT_FAILED.value, task_name=task_name).inc()
        return DispatchResult.PRE_COMMIT_FAILED

    from apps.worker.app.celery_app import celery_app

    last_exc: Exception | None = None
    try:
        celery_app.send_task(
            task_name, kwargs=task_kwargs, queue=queue,
            headers=headers if headers else None,
            task_id=task_id,
        )
        if outbox_msg is not None:
            try:
                from datetime import UTC, datetime
                outbox_msg.status = "sent"
                outbox_msg.completed_at = datetime.now(UTC)
                db.commit()
            except Exception:
                db.rollback()
        logger.info("dispatch.enqueued", log_event=log_event, celery_task_id=task_id, **task_kwargs)
        DISPATCH_RESULTS_TOTAL.labels(result=DispatchResult.SENT.value, task_name=task_name).inc()
        return DispatchResult.SENT
    except Exception as exc:
        last_exc = exc
        logger.warning(
            "dispatch.send_failed",
            log_event=log_event,
            celery_task_id=task_id,
            **task_kwargs,
        )

    # Inline send failed.
    if outbox_msg is not None:
        # The outbox message was committed in the same transaction as the
        # job record.  Leave the job in "queued" status so the poll_outbox
        # beat task (runs every 30s) can pick up the pending outbox message
        # and dispatch the Celery task.  The worker's _validate_task_ownership
        # handles the task-id mismatch via re-delivery claim logic.
        logger.warning(
            "dispatch.outbox_pending",
            log_event=log_event,
            task_name=task_name,
            celery_task_id=task_id,
            correlation_id=str(getattr(job, "id", None)),
            msg="Inline send failed; outbox will retry within 60s.",
            exc_info=last_exc,
        )
        DISPATCH_RESULTS_TOTAL.labels(result="outbox_pending", task_name=task_name).inc()
        return DispatchResult.ENQUEUE_FAILED
    else:
        # No outbox safety net — mark the job failed so the user gets
        # immediate feedback rather than a job stuck in "queued" forever.
        logger.exception(
            "dispatch.enqueue_failed_no_outbox",
            log_event=log_event,
            task_name=task_name,
            celery_task_id=task_id,
            exc_info=last_exc,
        )
        job.status = RunJobStatus.FAILED
        job.error_code = "enqueue_failed"
        job.error_message = "Unable to dispatch task to broker after retries."
        job.completed_at = datetime.now(UTC)
        try:
            db.commit()
        except Exception:
            db.rollback()
        DISPATCH_RESULTS_TOTAL.labels(result=DispatchResult.ENQUEUE_FAILED.value, task_name=task_name).inc()
        return DispatchResult.ENQUEUE_FAILED
