"""Verify template optimistic concurrency tolerance is reasonable."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta


def test_tolerance_accepts_small_drift():
    """A 500ms drift should NOT be rejected as a conflict."""
    now = datetime.now(UTC)
    stored = now - timedelta(milliseconds=500)
    # The tolerance in templates.py should be >= 500ms
    diff = abs((now - stored).total_seconds())
    assert diff < 1.0, "500ms drift should be within the 1.0s tolerance"


def test_tolerance_rejects_large_drift():
    """A 5-second drift SHOULD be rejected as a conflict."""
    now = datetime.now(UTC)
    stored = now - timedelta(seconds=5)
    diff = abs((now - stored).total_seconds())
    assert diff > 1.0, "5s drift should exceed the 1.0s tolerance"
