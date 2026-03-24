"""Tests that rate limiting infrastructure is active and reachable."""
from __future__ import annotations

import inspect
from types import SimpleNamespace

from backtestforecast.security.rate_limits import RateLimiter


def _memory_only_limiter() -> RateLimiter:
    settings = SimpleNamespace(
        rate_limit_fail_closed=False,
        rate_limit_prefix="test-rate-limit",
        rate_limit_memory_max_keys=1000,
        redis_cache_url="redis://localhost:6379/0",
        rate_limit_degraded_memory_fallback=False,
    )
    return RateLimiter(settings=settings)


def test_rate_limiter_initialization():
    """Rate limiter should be initialized and callable."""
    limiter = _memory_only_limiter()
    assert limiter is not None
    limiter._redis_retry_after = float("inf")
    limiter.check(bucket="test:bucket", actor_key="test-actor", limit=100, window_seconds=60)


def test_rate_limiter_blocks_when_exceeded():
    """Rate limiter should block after limit is exceeded."""
    from backtestforecast.errors import RateLimitError

    limiter = _memory_only_limiter()
    limiter.reset()
    limiter._redis_retry_after = float("inf")

    blocked = False
    try:
        for _ in range(10):
            limiter.check(bucket="test:tight", actor_key="test-actor", limit=3, window_seconds=60)
    except RateLimitError:
        blocked = True

    assert blocked, "Expected rate limiter to block after exceeding limit"


# ---------------------------------------------------------------------------
# Item 43: /v1/me is rate limited
# ---------------------------------------------------------------------------


def test_me_endpoint_calls_rate_limiter_check():
    """Verify the /v1/me endpoint applies rate limiting."""
    from apps.api.app.routers.me import get_me

    source = inspect.getsource(get_me)
    assert 'bucket="me:read"' in source
    assert "get_rate_limiter().check(" in source
