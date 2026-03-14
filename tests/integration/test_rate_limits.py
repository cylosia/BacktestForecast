"""Tests that rate limiting infrastructure is active and reachable."""
from __future__ import annotations

from backtestforecast.security.rate_limits import get_rate_limiter


def test_rate_limiter_initialization():
    """Rate limiter should be initialized and callable."""
    limiter = get_rate_limiter()
    assert limiter is not None
    limiter.check(bucket="test:bucket", actor_key="test-actor", limit=100, window_seconds=60)


def test_rate_limiter_blocks_when_exceeded():
    """Rate limiter should block after limit is exceeded."""
    from backtestforecast.errors import RateLimitError

    limiter = get_rate_limiter()
    limiter.reset()

    blocked = False
    try:
        for _ in range(10):
            limiter.check(bucket="test:tight", actor_key="test-actor", limit=3, window_seconds=60)
    except RateLimitError:
        blocked = True

    assert blocked, "Expected rate limiter to block after exceeding limit"
