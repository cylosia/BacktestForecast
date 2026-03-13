from __future__ import annotations

from enum import Enum


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PlanTier(str, Enum):
    FREE = "free"
    PRO = "pro"
    PREMIUM = "premium"
