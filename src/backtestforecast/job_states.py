"""Formal state machine for job status transitions.

Validates that status transitions follow the allowed graph. Use
``validate_transition()`` before updating job status to catch bugs
where code attempts an invalid transition (e.g., succeeded -> running).
"""
from __future__ import annotations

ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "queued": frozenset({"running", "failed", "cancelled"}),
    "running": frozenset({"succeeded", "failed", "cancelled"}),
    "succeeded": frozenset(),
    "failed": frozenset(),
    "cancelled": frozenset(),
    "expired": frozenset(),
}

EXPORT_ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "queued": frozenset({"running", "failed", "cancelled"}),
    "running": frozenset({"succeeded", "failed", "cancelled"}),
    "succeeded": frozenset({"expired"}),
    "failed": frozenset(),
    "cancelled": frozenset(),
    "expired": frozenset(),
}

TERMINAL_STATUSES: frozenset[str] = frozenset({"succeeded", "failed", "cancelled", "expired"})


class InvalidStatusTransition(ValueError):
    def __init__(self, from_status: str, to_status: str, context: str = "") -> None:
        ctx = f" ({context})" if context else ""
        super().__init__(f"Invalid status transition: {from_status} -> {to_status}{ctx}")
        self.from_status = from_status
        self.to_status = to_status


def validate_transition(
    current: str,
    target: str,
    *,
    context: str = "",
    strict: bool = False,
    job_type: str = "",
) -> bool:
    """Check whether transitioning from *current* to *target* is valid.

    Returns True if valid. When *strict* is True, raises
    ``InvalidStatusTransition`` instead of returning False.
    Pass ``job_type="export"`` to use the export-specific transition graph
    which allows ``succeeded -> expired``.
    """
    transitions = EXPORT_ALLOWED_TRANSITIONS if job_type == "export" else ALLOWED_TRANSITIONS
    allowed = transitions.get(current)
    if allowed is None:
        if strict:
            raise InvalidStatusTransition(current, target, context)
        return False
    if target in allowed:
        return True
    if strict:
        raise InvalidStatusTransition(current, target, context)
    return False


def is_terminal(status: str) -> bool:
    """Return True if *status* is a terminal (non-retriable) state."""
    return status in TERMINAL_STATUSES
