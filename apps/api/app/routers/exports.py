from __future__ import annotations

import io
import re
from typing import Generator
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, status
from fastapi.responses import Response
from sqlalchemy.orm import Session
from starlette.responses import StreamingResponse

from apps.api.app.dependencies import get_current_user, get_request_metadata
from apps.api.app.dispatch import dispatch_celery_task
from backtestforecast.config import get_settings
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.schemas.exports import CreateExportRequest, ExportJobResponse
from backtestforecast.security import get_rate_limiter
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
    get_rate_limiter().check(
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

    export_job = service.exports.get(job_response.id)
    if export_job is not None:
        dispatch_celery_task(
            db=db,
            job=export_job,
            task_name="exports.generate",
            task_kwargs={"export_job_id": str(job_response.id)},
            queue="exports",
            log_event="export",
            logger=logger,
        )

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
    safe_name = re.sub(r"[^a-zA-Z0-9._-]", "_", export_job.file_name)
    allowed_mime_types = {"text/csv", "text/csv; charset=utf-8", "application/pdf"}
    mime_type = export_job.mime_type if export_job.mime_type in allowed_mime_types else "application/octet-stream"
    def _chunk_bytes(data: bytes, chunk_size: int = 65536) -> Generator[bytes, None, None]:
        stream = io.BytesIO(data)
        while chunk := stream.read(chunk_size):
            yield chunk

    return StreamingResponse(
        _chunk_bytes(export_job.content_bytes),
        media_type=mime_type,
        headers={
            "Content-Disposition": f'attachment; filename="{safe_name}"',
        },
    )
