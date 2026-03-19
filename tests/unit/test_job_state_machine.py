"""Tests for the job status state machine."""
from __future__ import annotations

import pytest

from backtestforecast.job_states import (
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


def test_succeeded_to_expired():
    assert validate_transition("succeeded", "expired") is True


def test_strict_raises():
    with pytest.raises(InvalidStatusTransition, match="succeeded → running"):
        validate_transition("succeeded", "running", strict=True)


def test_terminal_statuses():
    assert is_terminal("succeeded") is True
    assert is_terminal("failed") is True
    assert is_terminal("cancelled") is True
    assert is_terminal("expired") is True
    assert is_terminal("queued") is False
    assert is_terminal("running") is False
