from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import AliasChoices, BaseModel, Field, field_validator

from backtestforecast.billing.entitlements import ExportFormat
from backtestforecast.schemas.backtests import RiskFreeRatePointResponse
from backtestforecast.schemas.common import CursorPaginatedResponse, JobStatus, sanitize_error_message


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
    run_id: UUID = Field(
        validation_alias=AliasChoices("backtest_run_id", "multi_symbol_run_id", "multi_step_run_id"),
    )
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
    risk_free_rate: Decimal | None = None
    risk_free_rate_source: str | None = None
    risk_free_rate_model: Literal["scalar", "curve_default", "unknown"] | None = None
    risk_free_rate_curve_points: list[RiskFreeRatePointResponse] = Field(default_factory=list)

    _sanitize = field_validator("error_message", mode="before")(sanitize_error_message)


class ExportJobListResponse(CursorPaginatedResponse):
    items: list[ExportJobResponse]
