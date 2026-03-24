"""Shared helpers for Celery task modules.

Extracted from the monolithic tasks.py to enable future splitting of task
definitions into per-domain modules (backtest_tasks.py, export_tasks.py, etc.)
without duplicating common logic.

Usage:
    from apps.worker.app.task_helpers import commit_then_publish
"""
from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from backtestforecast.events import publish_job_status

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = structlog.get_logger("worker.task_helpers")


def commit_then_publish(
    session: Session,
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


def close_owned_resource(resource: object | None, *, label: str) -> None:
    """Best-effort close for worker-owned resources created inside a task."""
    if resource is None:
        return
    close = getattr(resource, "close", None)
    if not callable(close):
        return
    try:
        close()
    except Exception:
        logger.exception("worker_resource.close_failed", label=label)
