from __future__ import annotations

from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, status
from fastapi.responses import Response
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_request_metadata
from apps.worker.app.celery_app import celery_app
from backtestforecast.config import get_settings
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.schemas.exports import CreateExportRequest, ExportJobResponse
from backtestforecast.security import rate_limiter
from backtestforecast.services.exports import ExportService

router = APIRouter(prefix="/exports", tags=["exports"])
settings = get_settings()
logger = structlog.get_logger("api.exports")


@router.post("", response_model=ExportJobResponse, status_code=status.HTTP_202_ACCEPTED)
def create_export(
    payload: CreateExportRequest,
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
) -> ExportJobResponse:
    rate_limiter.check(
        bucket="exports:create",
        actor_key=str(user.id),
        limit=settings.export_create_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    service = ExportService(db)
    job_response = service.enqueue_export(
        user,
        payload,
        request_id=metadata.request_id,
        ip_address=metadata.ip_address,
    )

    if job_response.status == "queued":
        try:
            celery_app.send_task(
                "exports.generate",
                kwargs={"export_job_id": str(job_response.id)},
                queue="exports",
            )
            logger.info("export.enqueued", export_job_id=str(job_response.id))
        except Exception:
            logger.exception("export.enqueue_failed", export_job_id=str(job_response.id))

    db.expire_all()
    return service.get_export_status(user, job_response.id)


@router.get("/{export_job_id}/status", response_model=ExportJobResponse)
def get_export_status(
    export_job_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> ExportJobResponse:
    return ExportService(db).get_export_status(user, export_job_id)


@router.get("/{export_job_id}")
def download_export(
    export_job_id: UUID,
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
) -> Response:
    export_job = ExportService(db).get_export_for_download(
        user,
        export_job_id,
        request_id=metadata.request_id,
        ip_address=metadata.ip_address,
    )
    safe_name = export_job.file_name.replace('"', "").replace("\\", "").replace("\r", "").replace("\n", "")
    return Response(
        content=export_job.content_bytes,
        media_type=export_job.mime_type,
        headers={
            "Content-Disposition": f'attachment; filename="{safe_name}"',
        },
    )
