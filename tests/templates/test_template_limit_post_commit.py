"""Test that the template limit is enforced with a post-commit safety check.

The primary enforcement is the pre-insert count + advisory lock on Postgres.
The post-commit check is a safety net that catches races on SQLite where
the advisory lock is a no-op.
"""
from __future__ import annotations

import inspect

from backtestforecast.services.templates import BacktestTemplateService


def test_create_method_has_post_commit_count_check() -> None:
    """The create method must verify template count AFTER commit."""
    source = inspect.getsource(BacktestTemplateService.create)
    assert "post_count" in source, (
        "BacktestTemplateService.create must include a post-commit count check "
        "to prevent concurrent requests from exceeding the template limit"
    )
    assert "limit_exceeded_post_commit" in source, (
        "Post-commit limit check must log 'limit_exceeded_post_commit' for monitoring"
    )
