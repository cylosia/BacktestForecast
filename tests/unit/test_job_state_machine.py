"""Tests for the job status state machine."""
from __future__ import annotations

import pytest

from backtestforecast.job_states import (
    ALLOWED_TRANSITIONS,
    EXPORT_ALLOWED_TRANSITIONS,
    InvalidStatusTransition,
    is_terminal,
    validate_transition,
)


def test_queued_to_running():
    assert validate_transition("queued", "running") is True


def test_queued_to_succeeded_not_allowed():
    assert validate_transition("queued", "succeeded") is False


def test_running_to_succeeded():
    assert validate_transition("running", "succeeded") is True


def test_running_to_failed():
    assert validate_transition("running", "failed") is True


def test_succeeded_to_running_not_allowed():
    assert validate_transition("succeeded", "running") is False


def test_succeeded_to_expired_blocked_for_non_export_jobs():
    """Non-export jobs must NOT transition from succeeded to expired."""
    assert validate_transition("succeeded", "expired") is False
    assert validate_transition("succeeded", "expired", job_type="backtest") is False
    assert validate_transition("succeeded", "expired", job_type="scan") is False
    assert validate_transition("succeeded", "expired", job_type="sweep") is False


def test_succeeded_to_expired_allowed_for_export_jobs():
    """Only export jobs may transition from succeeded to expired."""
    assert validate_transition("succeeded", "expired", job_type="export") is True


def test_export_transitions_are_superset():
    """EXPORT_ALLOWED_TRANSITIONS must contain all non-export transitions plus expired."""
    for status, allowed in ALLOWED_TRANSITIONS.items():
        export_allowed = EXPORT_ALLOWED_TRANSITIONS.get(status, frozenset())
        assert allowed <= export_allowed, (
            f"Export graph for '{status}' is missing transitions {allowed - export_allowed}"
        )


def test_strict_raises():
    with pytest.raises(InvalidStatusTransition, match="succeeded -> running"):
        validate_transition("succeeded", "running", strict=True)


def test_strict_raises_for_non_export_succeeded_to_expired():
    with pytest.raises(InvalidStatusTransition, match="succeeded -> expired"):
        validate_transition("succeeded", "expired", strict=True)


def test_strict_does_not_raise_for_export_succeeded_to_expired():
    assert validate_transition("succeeded", "expired", strict=True, job_type="export") is True


def test_terminal_statuses():
    assert is_terminal("succeeded") is True
    assert is_terminal("failed") is True
    assert is_terminal("cancelled") is True
    assert is_terminal("expired") is True
    assert is_terminal("queued") is False
    assert is_terminal("running") is False


def test_unknown_status_returns_false():
    assert validate_transition("bogus", "running") is False


def test_job_type_default_uses_standard_transitions():
    """Default job_type='' uses ALLOWED_TRANSITIONS (no succeeded->expired)."""
    assert validate_transition("succeeded", "expired", job_type="") is False
