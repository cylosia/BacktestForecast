"""Shared Celery dispatch-and-handle-failure logic for create endpoints.

Uses a transactional outbox pattern: an OutboxMessage row is written in the
same DB transaction as the job record.  After commit, the task is sent to
Celery optimistically.  If the send succeeds the outbox row is marked
"sent"; if it fails the row stays "pending" and the ``poll_outbox`` Celery
beat task will pick it up within 60 seconds.
"""
from __future__ import annotations

import datetime as dt
import enum
from typing import Protocol, runtime_checkable
from uuid import uuid4

import structlog
from sqlalchemy.orm import Session

from backtestforecast.observability.metrics import DISPATCH_RESULTS_TOTAL
from backtestforecast.observability.tracing import get_tracer
from backtestforecast.schemas.common import RunJobStatus


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
    dispatch_started_at: dt.datetime | None
    completed_at: dt.datetime | None


UTC = getattr(dt, "UTC", dt.UTC)
tracer = get_tracer(__name__)
_OUTBOX_TASK_ID_KEY = "__dispatch_task_id__"
_OUTBOX_HEADERS_KEY = "__dispatch_headers__"

# Backward-compatible aliases kept for contract tests that still import the
# original retry-tuning names from this module.
_SEND_MAX_ATTEMPTS = 3
_SEND_RETRY_DELAY = 0.5


def _normalize_task_kwargs(task_kwargs: dict[str, object]) -> dict[str, object]:
    return {
        key: (value if isinstance(value, str) or value is None or isinstance(value, dict) else str(value))
        for key, value in task_kwargs.items()
    }


def _encode_outbox_task_kwargs(
    task_kwargs: dict[str, object],
    *,
    task_id: str | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, object]:
    encoded = _normalize_task_kwargs(task_kwargs)
    if task_id:
        encoded[_OUTBOX_TASK_ID_KEY] = task_id
    if headers:
        encoded[_OUTBOX_HEADERS_KEY] = dict(headers)
    return encoded


def decode_outbox_task_kwargs(task_kwargs: dict[str, object]) -> tuple[dict[str, object], str | None, dict[str, str] | None]:
    public_kwargs = dict(task_kwargs)
    task_id_raw = public_kwargs.pop(_OUTBOX_TASK_ID_KEY, None)
    headers_raw = public_kwargs.pop(_OUTBOX_HEADERS_KEY, None)
    task_id = task_id_raw if isinstance(task_id_raw, str) and task_id_raw else None
    headers = None
    if isinstance(headers_raw, dict):
        headers = {
            str(key): str(value)
            for key, value in headers_raw.items()
            if isinstance(key, str) and isinstance(value, str)
        } or None
    return public_kwargs, task_id, headers


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
    same transaction, then commits.  After commit, makes one optimistic
    inline delivery attempt to Celery.

    - On success: outbox row marked ``"sent"``, returns ``SENT``.
    - On failure with outbox committed: job stays ``"queued"``, outbox
      stays ``"pending"`` for the ``poll_outbox`` beat task to recover.
    - On failure without outbox: job marked ``"failed"`` immediately.

    Returns a :class:`DispatchResult` indicating the outcome.
    """
    with tracer.start_as_current_span(f"{task_name}.enqueue_dispatch") as span:
        for k, v in list(task_kwargs.items()):
            if not isinstance(v, str):
                logger.warning("dispatch.non_string_kwarg", key=k, type=type(v).__name__)
                task_kwargs[k] = str(v)

        job_id = getattr(job, "id", None)
        if span is not None:
            span.set_attribute("job.id", str(job_id) if job_id is not None else "")
            span.set_attribute("celery.task_name", task_name)
            span.set_attribute("celery.queue", queue)

        if job.status != RunJobStatus.QUEUED or job.celery_task_id is not None:
            logger.info(
                "dispatch.dispatch_skipped",
                log_event=log_event,
                reason="idempotent_return",
                status=job.status,
                correlation_job_id=str(job_id) if job_id is not None else None,
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
        dispatch_started_at = dt.datetime.now(UTC)
        job.celery_task_id = task_id
        job.dispatch_started_at = dispatch_started_at
        if span is not None:
            span.set_attribute("celery.task_id", task_id)
            span.set_attribute("dispatch.started_at", dispatch_started_at.isoformat())

        # Write an OutboxMessage in the same transaction as the job update so
        # that if the process crashes after commit but before send_task, the
        # poll_outbox beat task will pick up the pending message.
        try:
            from backtestforecast.models import OutboxMessage
            outbox_msg = OutboxMessage(
                task_name=task_name,
                task_kwargs_json=_encode_outbox_task_kwargs(
                    task_kwargs,
                    task_id=task_id,
                    headers=headers if headers else None,
                ),
                queue=queue,
                status="pending",
                correlation_id=job_id,
            )
            db.add(outbox_msg)
            db.flush()
            logger.info(
                "dispatch.outbox_written",
                log_event=log_event,
                correlation_job_id=str(job_id) if job_id is not None else None,
                correlation_outbox_id=str(outbox_msg.id),
                celery_task_id=task_id,
                **task_kwargs,
            )
            if span is not None:
                span.set_attribute("outbox.id", str(outbox_msg.id))
        except Exception:
            db.rollback()
            logger.exception(
                "dispatch.outbox_write_failed",
                log_event=log_event,
                correlation_job_id=str(job_id) if job_id is not None else None,
                **task_kwargs,
            )
            try:
                from sqlalchemy import update as _sa_update
                if job_id is not None:
                    model_cls = type(job)
                    db.execute(
                        _sa_update(model_cls)
                        .where(model_cls.id == job_id)
                        .values(
                            status=RunJobStatus.FAILED,
                            error_code="enqueue_failed",
                            error_message="Unable to persist outbox state before dispatch.",
                        )
                    )
                    db.commit()
            except Exception:
                db.rollback()
            DISPATCH_RESULTS_TOTAL.labels(result=DispatchResult.PRE_COMMIT_FAILED.value, task_name=task_name).inc()
            return DispatchResult.PRE_COMMIT_FAILED

        try:
            db.commit()
        except Exception:
            db.rollback()
            logger.exception(
                f"{log_event}.pre_commit_failed",
                correlation_job_id=str(job_id) if job_id is not None else None,
                **task_kwargs,
            )
            try:
                from sqlalchemy import update as _sa_update
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
            try:
                outbox_msg.status = "sent"
                outbox_msg.completed_at = dt.datetime.now(UTC)
                db.commit()
            except Exception:
                db.rollback()
            logger.info(
                "dispatch.enqueued",
                log_event=log_event,
                correlation_job_id=str(job_id) if job_id is not None else None,
                correlation_outbox_id=str(outbox_msg.id),
                celery_task_id=task_id,
                **task_kwargs,
            )
            DISPATCH_RESULTS_TOTAL.labels(result=DispatchResult.SENT.value, task_name=task_name).inc()
            return DispatchResult.SENT
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "dispatch.send_failed",
                log_event=log_event,
                correlation_job_id=str(job_id) if job_id is not None else None,
                correlation_outbox_id=str(outbox_msg.id),
                celery_task_id=task_id,
                **task_kwargs,
            )

        logger.warning(
            "dispatch.outbox_pending",
            log_event=log_event,
            task_name=task_name,
            correlation_job_id=str(job_id) if job_id is not None else None,
            correlation_outbox_id=str(outbox_msg.id),
            celery_task_id=task_id,
            msg="Inline send failed; outbox will retry within 60s.",
            exc_info=last_exc,
        )
        DISPATCH_RESULTS_TOTAL.labels(result="outbox_pending", task_name=task_name).inc()
        return DispatchResult.ENQUEUE_FAILED


def dispatch_outbox_task(
    *,
    db: Session,
    task_name: str,
    task_kwargs: dict[str, str | None],
    queue: str,
    logger: structlog.stdlib.BoundLogger,
) -> DispatchResult:
    """Persist and dispatch a task that has no backing job row."""
    from backtestforecast.models import OutboxMessage

    task_id = str(uuid4())
    normalized_kwargs = _encode_outbox_task_kwargs(task_kwargs, task_id=task_id)
    try:
        outbox_msg = OutboxMessage(
            task_name=task_name,
            task_kwargs_json=normalized_kwargs,
            queue=queue,
            status="pending",
            correlation_id=None,
        )
        db.add(outbox_msg)
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("dispatch.generic_outbox_write_failed", task_name=task_name, queue=queue)
        DISPATCH_RESULTS_TOTAL.labels(result=DispatchResult.PRE_COMMIT_FAILED.value, task_name=task_name).inc()
        return DispatchResult.PRE_COMMIT_FAILED

    from apps.worker.app.celery_app import celery_app

    try:
        public_kwargs, persisted_task_id, headers = decode_outbox_task_kwargs(normalized_kwargs)
        celery_app.send_task(
            task_name,
            kwargs=public_kwargs,
            queue=queue,
            task_id=persisted_task_id,
            headers=headers,
        )
        outbox_msg.status = "sent"
        outbox_msg.completed_at = dt.datetime.now(UTC)
        db.commit()
        DISPATCH_RESULTS_TOTAL.labels(result=DispatchResult.SENT.value, task_name=task_name).inc()
        return DispatchResult.SENT
    except Exception as exc:
        db.rollback()
        logger.warning(
            "dispatch.generic_outbox_pending",
            task_name=task_name,
            queue=queue,
            exc_info=exc,
        )
        DISPATCH_RESULTS_TOTAL.labels(result="outbox_pending", task_name=task_name).inc()
        return DispatchResult.ENQUEUE_FAILED
