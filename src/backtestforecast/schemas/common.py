from __future__ import annotations

import re
from enum import Enum, StrEnum

from pydantic import BaseModel, Field


_SENSITIVE_PATTERNS = [
    re.compile(r"Traceback \(most recent call"),
    re.compile(
        r"\b(SELECT|INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE)\b.{0,2000}\b(FROM|INTO|SET|TABLE|WHERE|VALUES|INDEX)\b",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(r"psycopg|sqlalchemy\.exc|SQLSTATE|pg_catalog", re.IGNORECASE),
    re.compile(r"[A-Za-z]:\\(?:[^\s\\]+\\){2,}[^\s]*|/(?:home|usr|var|tmp|etc)/[^\s]+"),
    re.compile(r"https?://(?:localhost|127\.0\.0\.1|10\.\d+|172\.(?:1[6-9]|2\d|3[01])|192\.168)[^\s]*"),
    re.compile(r"redis(?:s)?://[^\s]+", re.IGNORECASE),
    re.compile(r"\b(password|secret|token|api_key|bearer|authorization)\b.*[:=]\s*\S+", re.IGNORECASE),
    re.compile(r"\b(sk_live_|sk_test_|pk_live_|pk_test_|whsec_)\w+", re.IGNORECASE),
]


def sanitize_error_message(msg: str | None) -> str | None:
    """Redact potentially sensitive details, then truncate error messages."""
    if msg is None:
        return None
    for pattern in _SENSITIVE_PATTERNS:
        if pattern.search(msg):
            return "An internal error occurred."
    if len(msg) > 500:
        msg = msg[:500] + "..."
    return msg


class RunJobStatus(StrEnum):
    """Status values for jobs that do not support expiration (backtests, scans, sweeps, analyses)."""
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobStatus(StrEnum):
    """Status values for jobs that support expiration (exports)."""
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class PlanTier(StrEnum):
    FREE = "free"
    PRO = "pro"
    PREMIUM = "premium"


class QuotaErrorDetail(BaseModel):
    """Extra fields attached to 403 quota_exceeded / feature_locked errors."""
    current_tier: str | None = None
    required_tier: str | None = None


class ErrorDetail(BaseModel):
    code: str = Field(max_length=128)
    message: str = Field(max_length=2000)
    request_id: str | None = Field(default=None, max_length=64)
    detail: QuotaErrorDetail | None = Field(
        default=None,
        description="Present on 403 quota/feature errors with current_tier and/or required_tier.",
    )
    details: list[dict[str, object]] | None = Field(
        default=None,
        max_length=20,
        description="Present on 422 validation errors with per-field error descriptions.",
    )


class ErrorResponse(BaseModel):
    """Standard error envelope returned by all API error handlers."""
    error: ErrorDetail
