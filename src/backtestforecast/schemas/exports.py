from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from backtestforecast.billing.entitlements import ExportFormat
from backtestforecast.schemas.common import JobStatus, sanitize_error_message


class CreateExportRequest(BaseModel):
    run_id: UUID
    export_format: ExportFormat = Field(alias="format")
    idempotency_key: str | None = Field(
        default=None,
        min_length=4,
        max_length=80,
        description="Optional client-generated key for retry-safe export creation. Retries with the same key return the existing export job instead of creating duplicates.",
    )

    model_config = {
        "populate_by_name": True,
        "extra": "forbid",
    }


class ExportJobResponse(BaseModel):
    model_config = {"from_attributes": True, "populate_by_name": True}

    id: UUID
    run_id: UUID = Field(alias="backtest_run_id")
    export_format: ExportFormat
    status: JobStatus
    file_name: str
    mime_type: str
    size_bytes: int = 0
    sha256_hex: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    created_at: datetime

    started_at: datetime | None = None
    completed_at: datetime | None = None
    expires_at: datetime | None = None

    _sanitize = field_validator("error_message", mode="before")(sanitize_error_message)


class ExportJobListResponse(BaseModel):
    items: list[ExportJobResponse]
    total: int = 0
    offset: int = 0
    limit: int = 50
    next_cursor: str | None = None
