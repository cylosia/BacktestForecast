from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import update
from sqlalchemy.orm import Session

from apps.api.app.dispatch import dispatch_celery_task
from backtestforecast.models import OutboxMessage
from backtestforecast.observability.metrics import ORPHAN_DETECTIONS_TOTAL

UTC = timezone.utc
_STALE_QUEUED_REUSE_AFTER = timedelta(minutes=15)


def redispatch_if_stale_queued(
    session: Session,
    job: Any,
    *,
    model_name: str,
    task_name: str,
    task_kwargs: dict[str, str],
    queue: str,
    log_event: str,
    logger: Any,
    request_id: str | None = None,
    traceparent: str | None = None,
) -> Any:
    """Re-dispatch a stale queued job reused through idempotency/dup detection.

    If the job is still queued after the stale threshold, clear any stale task
    claim, fail superseded pending outbox rows, and issue a fresh dispatch so a
    user retry can recover a stranded job without creating a duplicate record.
    """
    created_at = getattr(job, "created_at", None)
    if getattr(job, "status", None) != "queued" or created_at is None:
        return job
    if getattr(created_at, "tzinfo", None) is None:
        created_at = created_at.replace(tzinfo=UTC)

    now = datetime.now(UTC)
    if created_at >= now - _STALE_QUEUED_REUSE_AFTER:
        return job

    ORPHAN_DETECTIONS_TOTAL.labels(kind="queued_job", source="idempotency_reuse", model=model_name).inc()
    logger.warning(
        "dispatch.stale_queued_job_reused",
        model=model_name,
        job_id=str(getattr(job, "id", None)),
        created_at=created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at),
        stale_after_seconds=int(_STALE_QUEUED_REUSE_AFTER.total_seconds()),
    )

    model_cls = type(job)
    session.execute(
        update(OutboxMessage)
        .where(
            OutboxMessage.correlation_id == job.id,
            OutboxMessage.status == "pending",
        )
        .values(
            status="failed",
            error_message="Superseded by stale idempotency retry redispatch.",
            completed_at=now,
            updated_at=now,
        )
    )
    session.execute(
        update(model_cls)
        .where(model_cls.id == job.id, model_cls.status == "queued")
        .values(
            celery_task_id=None,
            error_code=None,
            error_message=None,
            updated_at=now,
        )
    )
    session.flush()
    session.refresh(job)

    if getattr(job, "status", None) != "queued":
        return job

    dispatch_celery_task(
        db=session,
        job=job,
        task_name=task_name,
        task_kwargs=task_kwargs,
        queue=queue,
        log_event=log_event,
        logger=logger,
        request_id=request_id,
        traceparent=traceparent,
    )
    session.refresh(job)
    return job
