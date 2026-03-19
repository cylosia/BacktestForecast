"""Shared helpers for Celery task modules.

Extracted from the monolithic tasks.py to enable future splitting of task
definitions into per-domain modules (backtest_tasks.py, export_tasks.py, etc.)
without duplicating common logic.

Usage:
    from apps.worker.app.task_helpers import (
        commit_then_publish,
        mark_job_failed,
        update_heartbeat,
        validate_task_ownership,
    )
"""
from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from backtestforecast.events import _VALID_TARGET_STATUSES, publish_job_status

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = structlog.get_logger("worker.task_helpers")


def commit_then_publish(
    session: "Session",
    job_type: str,
    job_id: UUID,
    status: str,
    *,
    metadata: dict[str, str] | None = None,
) -> None:
    """Commit the current session, then publish an SSE status event.

    Used for early-exit failure paths (entitlement revoked, quota exceeded)
    where the ORM object has already been mutated on the session.
    """
    try:
        session.commit()
    except Exception:
        logger.exception("commit_then_publish.commit_failed", job_type=job_type, job_id=str(job_id))
        session.rollback()
        return
    try:
        publish_job_status(job_type, job_id, status, metadata=metadata)
    except Exception:
        logger.warning("commit_then_publish.publish_failed", job_type=job_type, job_id=str(job_id), exc_info=True)


def mark_job_failed(
    session: "Session",
    model_cls: type,
    obj_id: UUID,
    *,
    error_code: str,
    error_message: str,
    allowed_from: tuple[str, ...] = ("queued", "running"),
) -> None:
    """Mark a job as failed if it is in one of the allowed source statuses."""
    from datetime import UTC, datetime
    from sqlalchemy import update

    obj = session.get(model_cls, obj_id)
    if obj is not None and getattr(obj, "status", None) in allowed_from:
        values: dict[str, object] = {
            "status": "failed",
            "error_message": error_message,
            "completed_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
        if hasattr(model_cls, "error_code"):
            values["error_code"] = error_code
        session.execute(
            update(model_cls)
            .where(model_cls.id == obj_id, model_cls.status.in_(allowed_from))
            .values(**values)
        )
        try:
            session.commit()
        except Exception:
            logger.exception("mark_job_failed.commit_failed", model=model_cls.__name__, obj_id=str(obj_id))
            session.rollback()


_TERMINAL_STATUSES = _VALID_TARGET_STATUSES | frozenset({"expired"})


def update_heartbeat(session: "Session", model_cls: type, obj_id: UUID) -> None:
    """Best-effort heartbeat update for long-running tasks."""
    from datetime import UTC, datetime
    from sqlalchemy import update
    try:
        nested = session.begin_nested()
        session.execute(
            update(model_cls)
            .where(model_cls.id == obj_id)
            .values(last_heartbeat_at=datetime.now(UTC))
        )
        nested.commit()
    except Exception:
        try:
            nested.rollback()
        except Exception:
            pass


def validate_task_ownership(session: "Session", model_cls: type, obj_id: UUID, expected_task_id: str | None) -> bool:
    """Return True if this Celery delivery owns the job, False if it's a duplicate."""
    from sqlalchemy import update

    if expected_task_id is None:
        return True
    obj = session.get(model_cls, obj_id)
    if obj is None:
        logger.warning("validate_task_ownership.obj_not_found", model=model_cls.__name__, obj_id=str(obj_id))
        return False
    stored = getattr(obj, "celery_task_id", None)
    if stored == expected_task_id:
        return True
    if stored is None:
        result = session.execute(
            update(model_cls)
            .where(model_cls.id == obj_id, model_cls.celery_task_id.is_(None))
            .values(celery_task_id=expected_task_id)
            .returning(model_cls.id)
        )
        claimed = result.fetchone() is not None
        try:
            session.commit()
        except Exception:
            session.rollback()
            return False
        if not claimed:
            return False
        session.refresh(obj)
        return True
    current_status = getattr(obj, "status", None)
    if current_status is not None and current_status not in _TERMINAL_STATUSES:
        result = session.execute(
            update(model_cls)
            .where(
                model_cls.id == obj_id,
                model_cls.celery_task_id == stored,
                model_cls.status.notin_(_TERMINAL_STATUSES),
            )
            .values(celery_task_id=expected_task_id)
            .returning(model_cls.id)
        )
        claimed = result.fetchone() is not None
        try:
            session.commit()
        except Exception:
            session.rollback()
            return False
        if claimed:
            logger.info(
                "validate_task_ownership.redelivery_claimed",
                model=model_cls.__name__,
                obj_id=str(obj_id),
                old_task_id=stored,
                new_task_id=expected_task_id,
            )
            session.refresh(obj)
            return True
    return False


def handle_task_app_error(
    self,
    session: "Session",
    model_cls: type,
    obj_id: UUID,
    exc,
    *,
    task_name: str,
    job_type: str,
    job_counter,
    task_counter,
) -> dict[str, str]:
    """Shared error handler for AppError in Celery tasks.

    Handles ExternalServiceError retry, marks job failed, and publishes events.
    Returns a result dict.
    """
    import random
    from datetime import UTC, datetime
    from backtestforecast.errors import ExternalServiceError

    if isinstance(exc, ExternalServiceError):
        session.rollback()
        session.expire_all()
        delay = int(60 * (self.request.retries + 1) * random.uniform(0.8, 1.2))
        try:
            raise self.retry(exc=exc, countdown=delay)
        except self.MaxRetriesExceededError:
            pass
    session.rollback()
    session.expire_all()
    obj = session.get(model_cls, obj_id)
    if obj is not None and obj.status in ("queued", "running"):
        obj.status = "failed"
        obj.error_code = exc.code
        obj.error_message = str(exc.message)[:500] if exc.message else None
        obj.completed_at = datetime.now(UTC)
        try:
            session.commit()
        except Exception:
            session.rollback()
    job_counter.labels(status="failed").inc()
    task_counter.labels(task_name=task_name, status="failed").inc()
    publish_job_status(job_type, obj_id, "failed", metadata={"error_code": exc.code})
    return {"status": "failed", "id": str(obj_id), "error_code": exc.code}
