from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

RUNNABLE_JOB_STATUSES = frozenset({"queued", "running"})
TERMINAL_JOB_STATUSES = frozenset({"succeeded", "failed", "cancelled", "expired"})


def deletion_blocked_message(resource_label: str) -> str:
    return (
        f"Cannot delete this {resource_label} while it is queued or running. "
        "Use cancel first, then retry delete after the status becomes a terminal state."
    )


def cancellation_blocked_message(resource_label: str) -> str:
    return (
        f"Only queued or running {resource_label}s can be cancelled. "
        "Refresh the job first if you expected it to still be active."
    )


def running_transition_values(*, now: datetime | None = None, **extra: Any) -> dict[str, Any]:
    effective_now = now or datetime.now(UTC)
    return {
        "status": "running",
        "started_at": effective_now,
        "completed_at": None,
        "error_code": None,
        "error_message": None,
        "updated_at": effective_now,
        **extra,
    }


def success_transition_values(*, now: datetime | None = None, **extra: Any) -> dict[str, Any]:
    effective_now = now or datetime.now(UTC)
    return {
        "status": "succeeded",
        "completed_at": effective_now,
        "updated_at": effective_now,
        **extra,
    }


def failure_transition_values(
    *,
    error_code: str,
    error_message: str,
    started: bool = False,
    now: datetime | None = None,
    **extra: Any,
) -> dict[str, Any]:
    effective_now = now or datetime.now(UTC)
    values = {
        "status": "failed",
        "error_code": error_code,
        "error_message": error_message,
        "completed_at": effective_now,
        "updated_at": effective_now,
        **extra,
    }
    if started:
        values.setdefault("started_at", effective_now)
    return values


def transition_job_to_running(job: Any) -> None:
    current_status = getattr(job, "status", None)
    if current_status in TERMINAL_JOB_STATUSES:
        raise ValueError(
            f"Cannot transition job from terminal status {current_status!r} to 'running'."
        )
    for field_name, value in running_transition_values().items():
        setattr(job, field_name, value)


def fail_job(
    job: Any,
    *,
    error_code: str,
    error_message: str,
    started: bool = False,
) -> None:
    for field_name, value in failure_transition_values(
        error_code=error_code,
        error_message=error_message,
        started=started,
    ).items():
        setattr(job, field_name, value)


def fail_job_if_active(
    job: Any,
    *,
    error_code: str,
    error_message: str,
    active_statuses: frozenset[str] = RUNNABLE_JOB_STATUSES,
    started: bool = False,
) -> bool:
    if getattr(job, "status", None) not in active_statuses:
        return False
    fail_job(
        job,
        error_code=error_code,
        error_message=error_message,
        started=started,
    )
    return True
