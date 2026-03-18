"""Shared Celery dispatch-and-handle-failure logic for create endpoints.

TODO: Implement transactional outbox pattern. The current approach commits
the DB record first, then sends the Celery task. A proper outbox would:
1. Write an OutboxMessage row in the same transaction as the job record
2. A separate poller would read pending OutboxMessages and send tasks
3. This eliminates the window where commit succeeds but task send fails
"""
from __future__ import annotations

from typing import Any
from uuid import uuid4

import structlog
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

    Commits the ``celery_task_id`` to the database **before** sending the
    task to the broker, so the worker never processes a job whose state
    has not been persisted.  If the subsequent task send fails, the job
    is marked ``"failed"`` with ``error_code="enqueue_failed"``.

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
        job.status = "failed"
        job.error_code = "enqueue_failed"
        job.error_message = "Unable to persist task state before dispatch."
        try:
            db.commit()
        except Exception:
            db.rollback()
        return

    try:
        celery_app.send_task(
            task_name, kwargs=task_kwargs, queue=queue,
            headers=headers if headers else None,
            task_id=task_id,
        )
    except Exception:
        logger.exception(f"{log_event}.enqueue_failed", celery_task_id=task_id, **task_kwargs)
        job.status = "failed"
        job.error_code = "enqueue_failed"
        job.error_message = "Unable to dispatch job. Please try again."
        try:
            db.commit()
        except Exception:
            logger.exception(f"{log_event}.enqueue_failed.commit_error", **task_kwargs)
            db.rollback()
        return

    logger.info(f"{log_event}.enqueued", celery_task_id=task_id, **task_kwargs)
