from __future__ import annotations

import re
from typing import Annotated, Generator
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.responses import Response
from sqlalchemy.orm import Session
from starlette.responses import StreamingResponse

from apps.api.app.dependencies import get_current_user, get_request_metadata
from apps.api.app.dispatch import dispatch_celery_task
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.schemas.exports import CreateExportRequest, ExportJobListResponse, ExportJobResponse
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.exports import ExportService

router = APIRouter(prefix="/exports", tags=["exports"])
logger = structlog.get_logger("api.exports")


@router.get("", response_model=ExportJobListResponse)
def list_exports(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    settings: Settings = Depends(get_settings),
) -> ExportJobListResponse:
    get_rate_limiter().check(
        bucket="exports:read",
        actor_key=str(user.id),
        limit=settings.export_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with ExportService(db) as service:
        jobs = service.exports.list_for_user(user.id, limit=limit, offset=offset)
        total = service.exports.count_for_user(user.id)
        return ExportJobListResponse(
            items=[service._to_response(j) for j in jobs],
            total=total,
            offset=offset,
            limit=limit,
        )


@router.post("", response_model=ExportJobResponse, status_code=status.HTTP_202_ACCEPTED)
def create_export(
    payload: CreateExportRequest,
    request: Request,
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> ExportJobResponse:
    if not settings.feature_exports_enabled:
        from backtestforecast.errors import FeatureLockedError
        raise FeatureLockedError("Exports are temporarily disabled.", required_tier="free")
    get_rate_limiter().check(
        bucket="exports:create",
        actor_key=str(user.id),
        limit=settings.export_create_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with ExportService(db) as service:
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
                request_id=metadata.request_id,
                traceparent=request.headers.get("traceparent"),
            )
        else:
            logger.error("export.post_enqueue_missing", export_job_id=str(job_response.id))

        db.expire_all()
        return service.get_export_status(user, job_response.id)


@router.get("/{export_job_id}/status", response_model=ExportJobResponse)
def get_export_status(
    export_job_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> ExportJobResponse:
    get_rate_limiter().check(
        bucket="exports:read",
        actor_key=str(user.id),
        limit=settings.export_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with ExportService(db) as service:
        return service.get_export_status(user, export_job_id)


@router.get(
    "/{export_job_id}",
    responses={
        200: {
            "description": "Exported file streamed as an attachment.",
            "content": {
                "text/csv": {"schema": {"type": "string", "format": "binary"}},
                "application/pdf": {"schema": {"type": "string", "format": "binary"}},
                "application/octet-stream": {"schema": {"type": "string", "format": "binary"}},
            },
            "headers": {
                "Content-Disposition": {
                    "schema": {"type": "string"},
                    "description": 'attachment; filename="<safe_name>"',
                },
            },
        },
    },
)
def download_export(
    export_job_id: UUID,
    request: Request,
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> Response:
    get_rate_limiter().check(
        bucket="exports:download",
        actor_key=str(user.id),
        limit=settings.export_create_rate_limit * 3,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with ExportService(db) as service:
        export_job = service.get_export_for_download(
            user,
            export_job_id,
            request_id=metadata.request_id,
            ip_address=metadata.ip_address,
        )
        safe_name = re.sub(r"[^a-zA-Z0-9._-]", "_", export_job.file_name)
        allowed_mime_types = {"text/csv", "text/csv; charset=utf-8", "application/pdf"}
        mime_type = export_job.mime_type if export_job.mime_type in allowed_mime_types else "application/octet-stream"

        storage_key = getattr(export_job, "storage_key", None)
        content = export_job.content_bytes

        if storage_key and content is None:
            try:
                from backtestforecast.exports.storage import get_storage, S3Storage

                s3_storage = get_storage(settings)
                if not isinstance(s3_storage, S3Storage):
                    raise NotImplementedError
                s3_obj = s3_storage.get_object(storage_key)
                content_length = s3_obj.get("ContentLength")

                if content_length is not None and export_job.size_bytes > 0 and content_length != export_job.size_bytes:
                    logger.warning(
                        "export.s3_size_mismatch",
                        export_job_id=str(export_job_id),
                        expected=export_job.size_bytes,
                        actual=content_length,
                    )

                headers = {
                    "Content-Disposition": f'attachment; filename="{safe_name}"; filename*=UTF-8\'\'{safe_name}',
                }
                if content_length is not None:
                    headers["Content-Length"] = str(content_length)

                _CHUNK_SIZE = 32_768  # 32 KB

                def _stream_s3() -> Generator[bytes, None, None]:
                    body = s3_obj["Body"]
                    try:
                        while True:
                            chunk = body.read(_CHUNK_SIZE)
                            if not chunk:
                                break
                            yield chunk
                    finally:
                        body.close()

                return StreamingResponse(
                    _stream_s3(),
                    media_type=mime_type,
                    headers=headers,
                )
            except NotImplementedError:
                pass
            except Exception:
                logger.warning("export.s3_stream_unavailable", export_job_id=str(export_job_id), exc_info=True)
                from backtestforecast.errors import ExternalServiceError
                raise ExternalServiceError("Export storage is temporarily unavailable. Please retry in a moment.")

        if not content:
            from backtestforecast.errors import NotFoundError
            raise NotFoundError("Export file is not available.")

        headers = {
            "Content-Disposition": f'attachment; filename="{safe_name}"; filename*=UTF-8\'\'{safe_name}',
            "Content-Length": str(len(content)),
        }

        _FALLBACK_CHUNK_SIZE = 32_768  # 32 KB

        def _chunk_bytes(data: bytes, chunk_size: int = _FALLBACK_CHUNK_SIZE) -> Generator[bytes, None, None]:
            offset = 0
            while offset < len(data):
                yield data[offset:offset + chunk_size]
                offset += chunk_size

        return StreamingResponse(
            _chunk_bytes(content),
            media_type=mime_type,
            headers=headers,
        )
