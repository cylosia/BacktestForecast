from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from backtestforecast.billing.entitlements import ExportFormat
from backtestforecast.schemas.common import sanitize_error_message


class CreateExportRequest(BaseModel):
    run_id: UUID
    export_format: ExportFormat = Field(alias="format")
    idempotency_key: str | None = Field(default=None, min_length=4, max_length=80)

    model_config = {
        "populate_by_name": True,
    }


class ExportJobResponse(BaseModel):
    model_config = {"from_attributes": True}

    id: UUID
    run_id: UUID
    export_format: str
    status: str
    file_name: str
    mime_type: str
    size_bytes: int = 0
    error_code: str | None = None
    error_message: str | None = None
    created_at: datetime

    _sanitize = field_validator("error_message", mode="before")(sanitize_error_message)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    expires_at: datetime | None = None
