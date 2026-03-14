from __future__ import annotations

from typing import Annotated
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_request_metadata
from apps.api.app.dispatch import dispatch_celery_task
from backtestforecast.config import get_settings
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.schemas.backtests import (
    BacktestRunDetailResponse,
    BacktestRunListResponse,
    BacktestRunStatusResponse,
    CompareBacktestsRequest,
    CompareBacktestsResponse,
    CreateBacktestRunRequest,
)
from backtestforecast.security import get_rate_limiter
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
    request: Request,
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
) -> BacktestRunDetailResponse:
    get_rate_limiter().check(
        bucket="backtests:create",
        actor_key=str(user.id),
        limit=settings.backtest_create_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    service = BacktestService(db)
    run = service.enqueue(user, payload)

    dispatch_celery_task(
        db=db,
        job=run,
        task_name="backtests.run",
        task_kwargs={"run_id": str(run.id)},
        queue="research",
        log_event="backtest",
        logger=logger,
        request_id=metadata.request_id,
        traceparent=request.headers.get("traceparent"),
    )

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


@router.get("/{run_id}/status", response_model=BacktestRunStatusResponse)
def get_backtest_status(
    run_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> BacktestRunStatusResponse:
    service = BacktestService(db)
    return service.get_run_status(user, run_id)


@router.get("/{run_id}", response_model=BacktestRunDetailResponse)
def get_backtest(
    run_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    trade_limit: int = Query(default=10_000, ge=0, le=50_000),
) -> BacktestRunDetailResponse:
    service = BacktestService(db)
    return service.get_run(user, run_id, trade_limit=trade_limit)
