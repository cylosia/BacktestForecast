from __future__ import annotations

import re
from enum import Enum, StrEnum

from pydantic import BaseModel


_SENSITIVE_PATTERNS = [
    re.compile(r"Traceback \(most recent call"),
    re.compile(
        r"\b(SELECT|INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE)\b.{0,500}\b(FROM|INTO|SET|TABLE|WHERE|VALUES|INDEX)\b",
        re.IGNORECASE,
    ),
    re.compile(r"psycopg|sqlalchemy\.exc|SQLSTATE|pg_catalog", re.IGNORECASE),
    re.compile(r"[A-Za-z]:\\(?:[^\s\\]+\\){2,}[^\s]*|/(?:home|usr|var|tmp|etc)/[^\s]+"),
    re.compile(r"https?://(?:localhost|127\.0\.0\.1|10\.\d+|172\.(?:1[6-9]|2\d|3[01])|192\.168)[^\s]*"),
    re.compile(r"redis(?:s)?://[^\s]+", re.IGNORECASE),
    re.compile(r"\b(password|secret|token|api_key|bearer|authorization)\b.*[:=]\s*\S+", re.IGNORECASE),
    re.compile(r"\b(sk_live_|sk_test_|pk_live_|pk_test_|whsec_)\w+", re.IGNORECASE),
]


def sanitize_error_message(msg: str | None) -> str | None:
    """Truncate and redact potentially sensitive details from error messages."""
    if msg is None:
        return None
    for pattern in _SENSITIVE_PATTERNS:
        if pattern.search(msg):
            return "An internal error occurred."
    if len(msg) > 500:
        msg = msg[:500] + "..."
    return msg


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


ExportJobStatus = JobStatus


class PlanTier(str, Enum):
    FREE = "free"
    PRO = "pro"
    PREMIUM = "premium"


class ErrorDetail(BaseModel):
    code: str
    message: str
    request_id: str | None = None
    details: list[dict[str, object]] | None = None


class ErrorResponse(BaseModel):
    """Standard error envelope returned by all API error handlers."""
    error: ErrorDetail
