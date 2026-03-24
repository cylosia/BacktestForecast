from __future__ import annotations

from typing import Annotated
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import Response
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_current_user_readonly, get_request_metadata
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db, get_readonly_db
from backtestforecast.errors import AppValidationError, FeatureLockedError
from backtestforecast.models import User
from backtestforecast.schemas.backtests import (
    BacktestRunDetailResponse,
    BacktestRunListResponse,
    BacktestRunStatusResponse,
    CompareBacktestsRequest,
    CompareBacktestsResponse,
    CreateBacktestRunRequest,
)
from backtestforecast.schemas.common import RemediationActionsResponse, sanitize_error_message
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.backtests import BacktestService
from backtestforecast.services.remediation_actions import build_job_remediation_actions

router = APIRouter(prefix="/backtests", tags=["backtests"])
logger = structlog.get_logger("api.backtests")


@router.get("", response_model=BacktestRunListResponse)
def list_backtests(
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_readonly_db),
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0, le=10_000)] = 0,
    cursor: Annotated[str | None, Query(max_length=200, description="Opaque cursor from a previous response's next_cursor field. When provided, offset is ignored.")] = None,
    settings: Settings = Depends(get_settings),
) -> BacktestRunListResponse:
    get_rate_limiter().check(
        bucket="backtests:read",
        actor_key=str(user.id),
        limit=settings.backtest_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with BacktestService(db) as service:
        return service.list_runs(user, limit=limit, offset=offset, cursor=cursor)


@router.post("", response_model=BacktestRunDetailResponse, status_code=status.HTTP_202_ACCEPTED)
def create_backtest(
    payload: CreateBacktestRunRequest,
    request: Request,
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> BacktestRunDetailResponse:
    if not settings.feature_backtests_enabled:
        raise FeatureLockedError("Backtesting is temporarily disabled.", required_tier="free")
    if not payload.entry_rules:
        raise AppValidationError("At least one entry rule is required for user-created backtests.")
    get_rate_limiter().check(
        bucket="backtests:create",
        actor_key=str(user.id),
        limit=settings.backtest_create_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with BacktestService(db) as service:
        run = service.create_and_dispatch(
            user,
            payload,
            request_id=metadata.request_id,
            traceparent=request.headers.get("traceparent"),
            dispatch_logger=logger,
        )
        if run.status == "failed":
            raise HTTPException(status_code=500, detail={"code": "enqueue_failed", "message": sanitize_error_message(run.error_message) or "Unable to dispatch job."})
        return service.get_run_for_owner(user_id=user.id, run_id=run.id)


@router.post("/compare", response_model=CompareBacktestsResponse)
def compare_backtests(
    payload: CompareBacktestsRequest,
    request: Request,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_readonly_db),
    settings: Settings = Depends(get_settings),
) -> CompareBacktestsResponse:
    if not settings.feature_backtests_enabled:
        raise FeatureLockedError("Backtesting is temporarily disabled.", required_tier="free")
    get_rate_limiter().check(
        bucket="backtests:compare",
        actor_key=str(user.id),
        limit=settings.backtest_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with BacktestService(db) as service:
        return service.compare_runs(user, payload)


@router.get("/{run_id}/status", response_model=BacktestRunStatusResponse)
def get_backtest_status(
    run_id: UUID,
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_readonly_db),
    settings: Settings = Depends(get_settings),
) -> BacktestRunStatusResponse:
    # Feature flag not checked on read: users may view past results even when creation is disabled.
    get_rate_limiter().check(
        bucket="backtests:read",
        actor_key=str(user.id),
        limit=settings.backtest_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with BacktestService(db) as service:
        return service.get_run_status(user, run_id)


@router.get("/{run_id}", response_model=BacktestRunDetailResponse)
def get_backtest(
    run_id: UUID,
    response: Response,
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_readonly_db),
    trade_limit: int = Query(default=10_000, ge=0, le=20_000),
    settings: Settings = Depends(get_settings),
) -> BacktestRunDetailResponse:
    # Feature flag not checked on read: users may view past results even when creation is disabled.
    get_rate_limiter().check(
        bucket="backtests:read",
        actor_key=str(user.id),
        limit=settings.backtest_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with BacktestService(db) as service:
        result = service.get_run_for_owner(user_id=user.id, run_id=run_id, trade_limit=trade_limit)
    response.headers["Cache-Control"] = "private, no-store"
    return result


@router.delete("/{run_id}", status_code=204)
def delete_backtest(
    run_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> None:
    """Delete a backtest run and its associated data."""
    # Feature flag not checked on read: users may view past results even when creation is disabled.
    get_rate_limiter().check(
        bucket="backtests:delete",
        actor_key=str(user.id),
        limit=settings.delete_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with BacktestService(db) as service:
        service.delete_for_user(run_id, user.id)


@router.post("/{run_id}/cancel", response_model=BacktestRunStatusResponse)
def cancel_backtest(
    run_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> BacktestRunStatusResponse:
    get_rate_limiter().check(
        bucket="backtests:delete",
        actor_key=str(user.id),
        limit=settings.delete_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with BacktestService(db) as service:
        return service.cancel_for_user(run_id, user.id)


@router.get("/{run_id}/remediation-actions", response_model=RemediationActionsResponse)
def get_backtest_remediation_actions(
    run_id: UUID,
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_readonly_db),
    settings: Settings = Depends(get_settings),
) -> RemediationActionsResponse:
    get_rate_limiter().check(
        bucket="backtests:read",
        actor_key=str(user.id),
        limit=settings.backtest_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with BacktestService(db) as service:
        status = service.get_run_status_for_owner(user_id=user.id, run_id=run_id)
    return build_job_remediation_actions(
        resource_type="backtest",
        resource_id=str(run_id),
        status=status.status,
        base_path=f"/v1/backtests/{run_id}",
    )
