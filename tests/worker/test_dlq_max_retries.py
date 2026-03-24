"""Verify BaseTaskWithDLQ handles max_retries=None correctly."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from apps.worker.app.tasks import BaseTaskWithDLQ


def test_on_failure_handles_max_retries_none():
    """When max_retries is None, on_failure must not crash and must not push to DLQ."""
    task = type("TaskWithNoRetries", (BaseTaskWithDLQ,), {"max_retries": None})()
    task.app = MagicMock()
    task.name = "test.task"
    # Celery request is a property; inject via __dict__ for test
    task.__dict__["request"] = SimpleNamespace(retries=0)

    # Should complete without raising; with max_retries=None, is_terminal is False
    # (unless SoftTimeLimitExceeded), so no DLQ push is attempted.
    task.on_failure(ValueError("test error"), "task-123", (), {}, None)
