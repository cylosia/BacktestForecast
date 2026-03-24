from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import structlog

logger = structlog.get_logger("services.job_cancellation")


def mark_job_cancelled(
    job: Any,
    *,
    error_code: str = "cancelled_by_user",
    error_message: str = "Cancelled by user.",
) -> str | None:
    now = datetime.now(UTC)
    task_id = getattr(job, "celery_task_id", None)
    job.status = "cancelled"
    job.completed_at = now
    job.updated_at = now
    job.error_code = error_code
    job.error_message = error_message
    return task_id


def revoke_celery_task(task_id: str | None, *, job_type: str, job_id: UUID) -> None:
    if not task_id:
        return
    try:
        from apps.worker.app.celery_app import celery_app

        celery_app.control.revoke(task_id, terminate=False)
    except Exception:
        logger.warning(
            "job_cancellation.celery_revoke_failed",
            job_type=job_type,
            job_id=str(job_id),
            task_id=task_id,
            exc_info=True,
        )


def publish_cancellation_event(
    *,
    job_type: str,
    job_id: UUID,
    error_code: str = "cancelled_by_user",
) -> None:
    try:
        from backtestforecast.events import publish_job_status

        publish_job_status(job_type, job_id, "cancelled", metadata={"error_code": error_code})
    except Exception:
        logger.debug("job_cancellation.publish_failed", job_type=job_type, job_id=str(job_id), exc_info=True)
