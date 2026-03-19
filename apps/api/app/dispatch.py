"""Shared Celery dispatch-and-handle-failure logic for create endpoints.

TODO: Implement transactional outbox pattern. The current approach commits
the DB record first, then sends the Celery task. A proper outbox would:
1. Write an OutboxMessage row in the same transaction as the job record
2. A separate poller would read pending OutboxMessages and send tasks
3. This eliminates the window where commit succeeds but task send fails

Recommended implementation approach:
- The OutboxMessage model already exists (see backtestforecast.models).
  The `poll_outbox` Celery beat task already re-sends stuck messages.
- To complete the pattern:
  a. In `dispatch_celery_task`, instead of commit → send_task, do:
     1) Create an OutboxMessage(task_name, task_kwargs, queue, status="pending")
        in the same transaction as the job record.
     2) Commit once (both job + outbox row are atomically persisted).
     3) Attempt `send_task` optimistically. On success, mark the outbox
        row as "sent". On failure, leave it as "pending" — the poller
        will pick it up within 60 seconds.
  b. This eliminates the current failure window: if the process crashes
     between commit and send_task, the outbox row survives and the
     poller retries delivery.
  c. Add a unique constraint on (correlation_id, task_name) to prevent
     duplicate sends if both the optimistic path and poller fire.
  d. The `poll_outbox` task already handles step (2) above; it just
     needs the outbox rows to be written transactionally.
"""
from __future__ import annotations

import enum
import time
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable
from uuid import uuid4

import structlog
from sqlalchemy.orm import Session

from backtestforecast.models import JobStatus, RunJobStatus

_SEND_MAX_ATTEMPTS = 2
_SEND_RETRY_DELAY = 0.25


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
    if job.status != RunJobStatus.QUEUED or job.celery_task_id is not None:
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
    task_id = str(uuid4())
    job.celery_task_id = task_id
    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception(f"{log_event}.pre_commit_failed", **task_kwargs)
        job.status = RunJobStatus.FAILED
        job.error_code = "enqueue_failed"
        job.error_message = "Unable to persist task state before dispatch."
        try:
            db.commit()
        except Exception:
            db.rollback()
        return DispatchResult.PRE_COMMIT_FAILED

    from apps.worker.app.celery_app import celery_app

    last_exc: Exception | None = None
    for attempt in range(1, _SEND_MAX_ATTEMPTS + 1):
        try:
            celery_app.send_task(
                task_name, kwargs=task_kwargs, queue=queue,
                headers=headers if headers else None,
                task_id=task_id,
            )
            logger.info("dispatch.enqueued", log_event=log_event, celery_task_id=task_id, attempt=attempt, **task_kwargs)
            return DispatchResult.SENT
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "dispatch.send_attempt_failed",
                log_event=log_event,
                celery_task_id=task_id,
                attempt=attempt,
                max_attempts=_SEND_MAX_ATTEMPTS,
                **task_kwargs,
            )
            if attempt < _SEND_MAX_ATTEMPTS:
                # Safe to use time.sleep here: this sync function runs in
                # FastAPI's default threadpool, so blocking does not stall
                # the async event loop.
                time.sleep(_SEND_RETRY_DELAY * attempt)

    logger.exception(
        "dispatch.enqueue_failed",
        log_event=log_event,
        celery_task_id=task_id,
        **task_kwargs,
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
    return DispatchResult.ENQUEUE_FAILED
