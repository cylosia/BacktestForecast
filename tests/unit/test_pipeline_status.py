"""Verify NightlyPipelineRun model allows 'cancelled' status."""
from __future__ import annotations

import pytest

from backtestforecast.models import NightlyPipelineRun


def test_cancelled_status_in_check_constraint():
    """The CHECK constraint must include 'cancelled'."""
    table_args = NightlyPipelineRun.__table_args__
    for arg in table_args:
        if hasattr(arg, "sqltext"):
            text = str(arg.sqltext)
            if "status" in text and "IN" in text.upper():
                assert "cancelled" in text, (
                    f"CHECK constraint missing 'cancelled': {text}"
                )
                return
    pytest.fail("No status CHECK constraint found on NightlyPipelineRun")
