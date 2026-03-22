from __future__ import annotations

from typing import Annotated
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_current_user_readonly, get_request_metadata
from backtestforecast.billing.entitlements import ensure_forecasting_access
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db, get_readonly_db
from backtestforecast.errors import FeatureLockedError
from backtestforecast.feature_flags import is_feature_enabled
from backtestforecast.models import User
from backtestforecast.schemas.common import sanitize_error_message
from backtestforecast.schemas.scans import (
    CreateScannerJobRequest,
    ScannerJobListResponse,
    ScannerJobResponse,
    ScannerJobStatusResponse,
    ScannerRecommendationListResponse,
)
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.dispatch_recovery import get_dispatch_diagnostic
from backtestforecast.services.scans import ScanService

logger = structlog.get_logger("api.scans")


def _require_scanner_enabled(settings: Settings = Depends(get_settings)) -> None:
    if not settings.feature_scanner_enabled:
        raise FeatureLockedError("Scanner is temporarily disabled.", required_tier="free")

router = APIRouter(prefix="/scans", tags=["scans"])


@router.get("", response_model=ScannerJobListResponse)
def list_scans(
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_readonly_db),
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    cursor: Annotated[str | None, Query(max_length=200, description="Opaque cursor from a previous response's next_cursor field. When provided, offset is ignored.")] = None,
    settings: Settings = Depends(get_settings),
) -> ScannerJobListResponse:
    get_rate_limiter().check(
        bucket="scans:read",
        actor_key=str(user.id),
        limit=settings.scan_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with ScanService(db) as service:
        return service.list_jobs(user, limit=limit, offset=offset, cursor=cursor)


@router.post("", response_model=ScannerJobResponse, status_code=status.HTTP_202_ACCEPTED)
def create_scan(
    payload: CreateScannerJobRequest,
    request: Request,
    user: User = Depends(get_current_user_readonly),
    _: None = Depends(_require_scanner_enabled),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> ScannerJobResponse:
    if not is_feature_enabled("scanner", user_id=user.id, plan_tier=user.plan_tier):
        raise FeatureLockedError("Scanner is temporarily disabled for this account.", required_tier="free")
    get_rate_limiter().check(
        bucket="scans:create",
        actor_key=str(user.id),
        limit=settings.scan_create_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
    with ScanService(db) as service:
        job = service.create_and_dispatch_job(
            user,
            payload,
            request_id=metadata.request_id,
            traceparent=request.headers.get("traceparent"),
            dispatch_logger=logger,
        )
        if job.status == "failed":
            raise HTTPException(status_code=500, detail={"code": "enqueue_failed", "message": sanitize_error_message(job.error_message) or "Unable to dispatch job."})
        return service.get_job(user, job.id)


@router.get("/{job_id}/status", response_model=ScannerJobStatusResponse)
def get_scan_status(
    job_id: UUID,
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_readonly_db),
    settings: Settings = Depends(get_settings),
) -> ScannerJobStatusResponse:
    get_rate_limiter().check(
        bucket="scans:read",
        actor_key=str(user.id),
        limit=settings.scan_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with ScanService(db) as service:
        job = service.get_job(user, job_id)
        diagnostic = get_dispatch_diagnostic(job)
        return ScannerJobStatusResponse(
            id=job.id,
            status=job.status,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            error_code=job.error_code or (diagnostic[0] if diagnostic else None),
            error_message=job.error_message or (diagnostic[1] if diagnostic else None),
        )


@router.get("/{job_id}", response_model=ScannerJobResponse)
def get_scan(
    job_id: UUID,
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_readonly_db),
    settings: Settings = Depends(get_settings),
) -> ScannerJobResponse:
    get_rate_limiter().check(
        bucket="scans:read",
        actor_key=str(user.id),
        limit=settings.scan_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with ScanService(db) as service:
        return service.get_job(user, job_id)


@router.delete("/{job_id}", status_code=204)
def delete_scan(
    job_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> None:
    """Delete a scan job and its recommendations."""
    get_rate_limiter().check(
        bucket="scans:delete",
        actor_key=str(user.id),
        limit=settings.delete_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with ScanService(db) as service:
        service.delete_for_user(job_id, user.id)


@router.get("/{job_id}/recommendations", response_model=ScannerRecommendationListResponse)
def get_scan_recommendations(
    job_id: UUID,
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_readonly_db),
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    settings: Settings = Depends(get_settings),
) -> ScannerRecommendationListResponse:
    get_rate_limiter().check(
        bucket="scans:read",
        actor_key=str(user.id),
        limit=settings.scan_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with ScanService(db) as service:
        return service.get_recommendations(user, job_id, limit=limit, offset=offset)
