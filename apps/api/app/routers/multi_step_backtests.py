from __future__ import annotations

from typing import Annotated
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_current_user_readonly, get_request_metadata
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db
from backtestforecast.errors import FeatureLockedError
from backtestforecast.models import User
from backtestforecast.schemas.common import sanitize_error_message
from backtestforecast.schemas.multi_step_backtests import (
    CreateMultiStepRunRequest,
    MultiStepRunDetailResponse,
    MultiStepRunListResponse,
    MultiStepRunStatusResponse,
)
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.multi_step_backtests import MultiStepBacktestService

router = APIRouter(prefix="/multi-step-backtests", tags=["multi-step-backtests"])
logger = structlog.get_logger("api.multi_step_backtests")


@router.get("", response_model=MultiStepRunListResponse)
def list_multi_step_backtests(
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_db),
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0, le=10_000)] = 0,
    cursor: Annotated[str | None, Query(max_length=200)] = None,
    settings: Settings = Depends(get_settings),
) -> MultiStepRunListResponse:
    get_rate_limiter().check(
        bucket="multi_step_backtests:read",
        actor_key=str(user.id),
        limit=settings.backtest_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with MultiStepBacktestService(db) as service:
        return service.list_runs(user, limit=limit, offset=offset, cursor=cursor)


@router.post("", response_model=MultiStepRunDetailResponse, status_code=status.HTTP_202_ACCEPTED)
def create_multi_step_backtest(
    payload: CreateMultiStepRunRequest,
    request: Request,
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> MultiStepRunDetailResponse:
    if not settings.feature_backtests_enabled:
        raise FeatureLockedError("Backtesting is temporarily disabled.", required_tier="free")
    get_rate_limiter().check(
        bucket="multi_step_backtests:create",
        actor_key=str(user.id),
        limit=settings.backtest_create_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with MultiStepBacktestService(db) as service:
        run = service.create_and_dispatch(
            user,
            payload,
            request_id=metadata.request_id,
            traceparent=request.headers.get("traceparent"),
            dispatch_logger=logger,
        )
        if run.status == "failed":
            raise HTTPException(
                status_code=500,
                detail={"code": "enqueue_failed", "message": sanitize_error_message(run.error_message) or "Unable to dispatch job."},
            )
        return service.get_run_for_owner(user_id=user.id, run_id=run.id)


@router.get("/{run_id}", response_model=MultiStepRunDetailResponse)
def get_multi_step_backtest(
    run_id: UUID,
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> MultiStepRunDetailResponse:
    get_rate_limiter().check(
        bucket="multi_step_backtests:read",
        actor_key=str(user.id),
        limit=settings.backtest_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with MultiStepBacktestService(db) as service:
        return service.get_run_for_owner(user_id=user.id, run_id=run_id)


@router.get("/{run_id}/status", response_model=MultiStepRunStatusResponse)
def get_multi_step_backtest_status(
    run_id: UUID,
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> MultiStepRunStatusResponse:
    get_rate_limiter().check(
        bucket="multi_step_backtests:read",
        actor_key=str(user.id),
        limit=settings.backtest_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with MultiStepBacktestService(db) as service:
        return service.get_run_status_for_owner(user_id=user.id, run_id=run_id)
