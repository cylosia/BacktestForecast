from __future__ import annotations

import re
from enum import Enum


def sanitize_error_message(msg: str | None) -> str | None:
    """Truncate and redact potentially sensitive details from error messages."""
    if msg is None:
        return None
    if re.search(r"Traceback \(most recent call", msg):
        return "An internal error occurred."
    if len(msg) > 500:
        msg = msg[:500] + "..."
    return msg


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class PlanTier(str, Enum):
    FREE = "free"
    PRO = "pro"
    PREMIUM = "premium"
