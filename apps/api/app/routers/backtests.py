from __future__ import annotations

from typing import Annotated
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_request_metadata
from apps.worker.app.celery_app import celery_app
from backtestforecast.config import get_settings
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.schemas.backtests import (
    BacktestRunDetailResponse,
    BacktestRunListResponse,
    CompareBacktestsRequest,
    CompareBacktestsResponse,
    CreateBacktestRunRequest,
)
from backtestforecast.security import rate_limiter
from backtestforecast.services.backtests import BacktestService

router = APIRouter(prefix="/backtests", tags=["backtests"])
settings = get_settings()
logger = structlog.get_logger("api.backtests")


@router.get("", response_model=BacktestRunListResponse)
def list_backtests(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> BacktestRunListResponse:
    service = BacktestService(db)
    return service.list_runs(user, limit=limit)


@router.post("", response_model=BacktestRunDetailResponse, status_code=status.HTTP_202_ACCEPTED)
def create_backtest(
    payload: CreateBacktestRunRequest,
    user: User = Depends(get_current_user),
    _metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
) -> BacktestRunDetailResponse:
    rate_limiter.check(
        bucket="backtests:create",
        actor_key=str(user.id),
        limit=settings.backtest_create_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    service = BacktestService(db)
    run = service.enqueue(user, payload)

    # Dispatch to Celery worker if run is newly created (queued)
    if run.status == "queued" and run.celery_task_id is None:
        try:
            result = celery_app.send_task(
                "backtests.run",
                kwargs={"run_id": str(run.id)},
                queue="research",
            )
            service.set_celery_task_id(run.id, result.id)
            logger.info(
                "backtest.enqueued",
                run_id=str(run.id),
                celery_task_id=result.id,
            )
        except Exception:
            logger.exception("backtest.enqueue_failed", run_id=str(run.id))
            run.status = "failed"
            run.error_code = "enqueue_failed"
            run.error_message = "Unable to dispatch job. Please try again."
            db.commit()

    db.expire_all()
    return service.get_run(user, run.id)


@router.post("/compare", response_model=CompareBacktestsResponse)
def compare_backtests(
    payload: CompareBacktestsRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> CompareBacktestsResponse:
    service = BacktestService(db)
    return service.compare_runs(user, payload)


@router.get("/{run_id}", response_model=BacktestRunDetailResponse)
def get_backtest(
    run_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> BacktestRunDetailResponse:
    service = BacktestService(db)
    return service.get_run(user, run_id)
