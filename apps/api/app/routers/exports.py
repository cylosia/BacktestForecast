from __future__ import annotations

import re
import time as _time
from typing import Annotated, Generator
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import Response
from sqlalchemy.orm import Session
from starlette.responses import StreamingResponse

from apps.api.app.dependencies import get_current_user, get_request_metadata
from apps.api.app.dispatch import dispatch_celery_task
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.schemas.common import sanitize_error_message
from backtestforecast.schemas.exports import CreateExportRequest, ExportJobListResponse, ExportJobResponse
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.exports import ExportService

_MAX_EXPORT_SIZE_BYTES = 100 * 1024 * 1024  # 100 MB

router = APIRouter(prefix="/exports", tags=["exports"])
logger = structlog.get_logger("api.exports")


@router.get("", response_model=ExportJobListResponse)
def list_exports(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    cursor: Annotated[str | None, Query(max_length=200)] = None,
    settings: Settings = Depends(get_settings),
) -> ExportJobListResponse:
    get_rate_limiter().check(
        bucket="exports:read",
        actor_key=str(user.id),
        limit=settings.export_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    from backtestforecast.utils import decode_cursor, encode_cursor

    cursor_before = None
    if cursor:
        cursor_before = decode_cursor(cursor)
        if cursor_before is None:
            from backtestforecast.errors import ValidationError
            raise ValidationError("Invalid pagination cursor.")
        offset = 0

    with ExportService(db) as service:
        jobs, total = service.exports.list_for_user_with_count(
            user.id, limit=limit + 1, offset=offset, cursor_before=cursor_before,
        )
        has_next = len(jobs) > limit
        if has_next:
            jobs = jobs[:limit]
        next_cursor = encode_cursor(jobs[-1].created_at) if has_next and jobs else None
        return ExportJobListResponse(
            items=[service.to_response(j) for j in jobs],
            total=total,
            offset=offset,
            limit=limit,
            next_cursor=next_cursor,
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

        export_job = service.exports.get_for_user(job_response.id, user.id)
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
            from backtestforecast.errors import AppError
            raise AppError(code="enqueue_failed", message="Export job could not be verified after creation.")

        if export_job is not None:
            db.refresh(export_job)
            if export_job.status == "failed":
                raise HTTPException(status_code=500, detail={"code": "enqueue_failed", "message": sanitize_error_message(export_job.error_message) or "Unable to dispatch job."})
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


@router.delete("/{export_job_id}", status_code=204)
def delete_export(
    export_job_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> None:
    """Delete an export job."""
    get_rate_limiter().check(
        bucket="exports:delete",
        actor_key=str(user.id),
        limit=settings.delete_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with ExportService(db) as service:
        service.delete_for_user(export_job_id, user.id)


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
        safe_name = re.sub(r"[^a-zA-Z0-9._-]", "_", export_job.file_name.replace("\r", "").replace("\n", ""))
        safe_name = safe_name.lstrip(".").replace("..", "_") or "export"
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
                    logger.error(
                        "export.s3_size_mismatch",
                        export_job_id=str(export_job_id),
                        expected=export_job.size_bytes,
                        actual=content_length,
                    )
                    from backtestforecast.errors import ExternalServiceError
                    raise ExternalServiceError(
                        "Export file integrity check failed. Please re-export."
                    )

                if content_length is not None and content_length > _MAX_EXPORT_SIZE_BYTES:
                    logger.error(
                        "export.s3_size_exceeded",
                        export_job_id=str(export_job_id),
                        content_length=content_length,
                        max_allowed=_MAX_EXPORT_SIZE_BYTES,
                    )
                    from fastapi import HTTPException
                    raise HTTPException(
                        status_code=500,
                        detail="Export file exceeds maximum allowed size.",
                    )

                headers = {
                    "Content-Disposition": f'attachment; filename="{safe_name}"; filename*=UTF-8\'\'{safe_name}',
                    "X-Accel-Buffering": "no",
                    "X-Content-Type-Options": "nosniff",
                    "Cache-Control": "no-store, no-cache, must-revalidate",
                    "Transfer-Encoding": "chunked",
                }
                if content_length is not None and content_length <= 5 * 1024 * 1024:
                    headers["Content-Length"] = str(content_length)
                    del headers["Transfer-Encoding"]

                _CHUNK_SIZE = 32_768  # 32 KB
                _STREAM_TIMEOUT_SECONDS = 300  # 5 minutes

                def _stream_s3() -> Generator[bytes, None, None]:
                    body = s3_obj["Body"]
                    stream_start = _time.monotonic()
                    try:
                        while True:
                            elapsed = _time.monotonic() - stream_start
                            if elapsed > _STREAM_TIMEOUT_SECONDS:
                                logger.error(
                                    "export.stream_timeout",
                                    export_job_id=str(export_job_id),
                                    elapsed_seconds=round(elapsed, 1),
                                )
                                raise TimeoutError(
                                    f"S3 stream exceeded {_STREAM_TIMEOUT_SECONDS}s timeout"
                                )
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

        # TODO: For large exports (>10MB), consider using StreamingResponse with chunked
        # reads from the database to avoid loading the full file into memory.
        # Currently bounded by _MAX_EXPORT_SIZE_BYTES but could still cause memory
        # pressure under concurrent downloads.
        #
        # Recommended implementation approach:
        # 1. Check `len(content_bytes)` against a 10MB threshold.
        # 2. If exceeded, use `StreamingResponse` with a generator that yields
        #    32KB chunks from the in-memory bytes (already done below via
        #    `_chunk_bytes`). The real win comes from avoiding the full DB
        #    column load — requires switching to server-side cursor or
        #    PostgreSQL large-object streaming (lo_read) so the content is
        #    never fully materialized in Python memory.
        # 3. For DB-backed exports, consider migrating all large files to S3
        #    (storage_key path) so this fallback path only handles small files.
        # 4. Add a `Content-Length` header from `export_job.size_bytes` so
        #    clients can show download progress.

        if content is None:
            from backtestforecast.errors import NotFoundError
            raise NotFoundError("Export file is not available.")

        if len(content) > _MAX_EXPORT_SIZE_BYTES:
            from backtestforecast.errors import AppValidationError
            raise AppValidationError("Export file exceeds maximum allowed size.")

        if export_job.sha256_hex:
            import hashlib
            actual_hash = hashlib.sha256(content).hexdigest()
            if actual_hash != export_job.sha256_hex:
                logger.error(
                    "export.integrity_check_failed",
                    export_job_id=str(export_job_id),
                    expected_sha256=export_job.sha256_hex[:16],
                    actual_sha256=actual_hash[:16],
                )
                from backtestforecast.errors import ExternalServiceError
                raise ExternalServiceError("Export file integrity check failed. Please re-export.")

        headers = {
            "Content-Disposition": f'attachment; filename="{safe_name}"; filename*=UTF-8\'\'{safe_name}',
            "Content-Length": str(len(content)),
            "X-Accel-Buffering": "no",
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "no-store, no-cache, must-revalidate",
        }
        if export_job.sha256_hex:
            headers["ETag"] = f'"{export_job.sha256_hex[:32]}"'

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
