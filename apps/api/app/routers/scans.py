from __future__ import annotations

from typing import Annotated
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_request_metadata
from apps.api.app.dispatch import dispatch_celery_task
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.schemas.scans import (
    CreateScannerJobRequest,
    ScannerJobListResponse,
    ScannerJobResponse,
    ScannerRecommendationListResponse,
)
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.scans import ScanService

logger = structlog.get_logger("api.scans")

router = APIRouter(prefix="/scans", tags=["scans"])


@router.get("", response_model=ScannerJobListResponse)
def list_scans(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    settings: Settings = Depends(get_settings),
) -> ScannerJobListResponse:
    get_rate_limiter().check(
        bucket="scans:read",
        actor_key=str(user.id),
        limit=settings.scan_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with ScanService(db) as service:
        return service.list_jobs(user, limit=limit, offset=offset)


@router.post("", response_model=ScannerJobResponse, status_code=status.HTTP_202_ACCEPTED)
def create_scan(
    payload: CreateScannerJobRequest,
    request: Request,
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> ScannerJobResponse:
    if not settings.feature_scanner_enabled:
        from backtestforecast.errors import FeatureLockedError
        raise FeatureLockedError("Scanner is temporarily disabled.", required_tier="free")
    get_rate_limiter().check(
        bucket="scans:create",
        actor_key=str(user.id),
        limit=settings.scan_create_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with ScanService(db) as service:
        job = service.create_job(user, payload)
        dispatch_celery_task(
            db=db,
            job=job,
            task_name="scans.run_job",
            task_kwargs={"job_id": str(job.id)},
            queue="research",
            log_event="scan",
            logger=logger,
            request_id=metadata.request_id,
            traceparent=request.headers.get("traceparent"),
        )
        db.refresh(job)
        if job.status == "failed":
            from fastapi import HTTPException
            raise HTTPException(status_code=500, detail={"code": "enqueue_failed", "message": job.error_message or "Unable to dispatch job."})
        return service.get_job(user, job.id)


@router.get("/{job_id}", response_model=ScannerJobResponse)
def get_scan(
    job_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
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


@router.get("/{job_id}/recommendations", response_model=ScannerRecommendationListResponse)
def get_scan_recommendations(
    job_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
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
