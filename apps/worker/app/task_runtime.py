from __future__ import annotations

from contextlib import suppress
from datetime import UTC, datetime
from random import SystemRandom
from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from backtestforecast.events import _VALID_TARGET_STATUSES, publish_job_status

if TYPE_CHECKING:
    from datetime import date

    from sqlalchemy.orm import Session

logger = structlog.get_logger("worker.tasks")
_retry_rng = SystemRandom()
_TERMINAL_STATUSES = _VALID_TARGET_STATUSES | frozenset({"expired"})


def compute_retry_delay(base_delay_seconds: int, retries: int) -> int:
    return int(base_delay_seconds * (retries + 1) * _retry_rng.uniform(0.8, 1.2))


def publish_job_status_safe(
    target: str,
    obj_id: UUID,
    status: str,
    *,
    metadata: dict[str, object] | None = None,
    log_event: str,
    **log_fields: object,
) -> None:
    try:
        publish_job_status(target, obj_id, status, metadata=metadata)
    except Exception:
        logger.warning(log_event, exc_info=True, **log_fields)


def find_pipeline_run[ModelT](
    session: Session,
    model_cls: type[ModelT],
    run: ModelT | None,
    trade_date: date,
    *,
    run_id: UUID | None = None,
) -> ModelT | None:
    """Return the pipeline run object for failure marking."""
    effective_id = run_id or (run.id if run is not None else None)
    if effective_id is not None:
        return session.get(model_cls, effective_id)
    from sqlalchemy import desc, func, select

    running_count = session.scalar(
        select(func.count()).select_from(model_cls).where(
            model_cls.trade_date == trade_date, model_cls.status == "running"
        )
    ) or 0
    logger.error(
        "pipeline.find_run_fallback",
        trade_date=str(trade_date),
        running_count=running_count,
        msg=(
            "No run_id available; falling back to heuristic date-based lookup. "
            "Investigate why run_id was not captured."
        ),
    )
    if running_count > 1:
        logger.error(
            "pipeline.find_run_ambiguous",
            trade_date=str(trade_date),
            running_count=running_count,
            msg="Multiple running pipeline runs for this date - refusing to guess.",
        )
        return None
    stmt = (
        select(model_cls)
        .where(model_cls.trade_date == trade_date, model_cls.status == "running")
        .order_by(desc(model_cls.created_at))
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    return session.scalar(stmt)


def update_heartbeat(session: Session, model_cls: type, obj_id: UUID) -> None:
    """Best-effort heartbeat update for long-running tasks."""
    from sqlalchemy import update

    nested = None
    try:
        nested = session.begin_nested()
        session.execute(
            update(model_cls)
            .where(model_cls.id == obj_id)
            .values(last_heartbeat_at=datetime.now(UTC))
        )
        nested.commit()
    except Exception:
        if nested is not None:
            with suppress(Exception):
                nested.rollback()


def validate_task_ownership(
    session: Session,
    model_cls: type,
    obj_id: UUID,
    expected_task_id: str | None,
) -> bool:
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
        nested = session.begin_nested()
        result = session.execute(
            update(model_cls)
            .where(model_cls.id == obj_id, model_cls.celery_task_id.is_(None))
            .values(celery_task_id=expected_task_id)
            .returning(model_cls.id)
        )
        claimed = result.fetchone() is not None
        try:
            nested.commit()
        except Exception:
            nested.rollback()
            logger.warning("validate_task_ownership.commit_failed", model=model_cls.__name__, obj_id=str(obj_id), exc_info=True)
            return False
        if not claimed:
            return False
        session.refresh(obj)
        return True
    current_status = getattr(obj, "status", None)
    if current_status is not None and current_status not in _TERMINAL_STATUSES:
        logger.info(
            "validate_task_ownership.superseded_delivery",
            model=model_cls.__name__,
            obj_id=str(obj_id),
            stored_task_id=stored,
            delivery_task_id=expected_task_id,
            status=current_status,
        )
    return False
