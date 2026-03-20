"""Verify in-memory rate limit fallback behavior when Redis is unavailable."""
from __future__ import annotations
from unittest.mock import MagicMock
from backtestforecast.security.rate_limits import RateLimiter


def _make_limiter(*, fail_closed: bool = False) -> RateLimiter:
    settings = MagicMock()
    settings.redis_cache_url = "redis://nonexistent:6379/0"
    settings.rate_limit_prefix = "test"
    settings.rate_limit_fail_closed = fail_closed
    settings.rate_limit_degraded_memory_fallback = False
    settings.rate_limit_memory_max_keys = 1000
    limiter = RateLimiter.__new__(RateLimiter)
    limiter.settings = settings
    limiter._fail_closed = fail_closed
    limiter._memory_lock = __import__("threading").Lock()
    limiter._redis_lock = __import__("threading").Lock()
    limiter._memory_counters = {}
    limiter._redis = None
    limiter._redis_retry_after = float("inf")  # force Redis unavailable
    limiter._lua_sha = None
    return limiter


class TestInMemoryFallbackMultiplication:
    def test_two_instances_have_independent_counters(self):
        limiter_a = _make_limiter()
        limiter_b = _make_limiter()

        for _ in range(5):
            limiter_a.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
            limiter_b.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)

        info_a = limiter_a.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        info_b = limiter_b.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)

        # Each instance counts independently — effective limit is 2x
        assert info_a.remaining == 4
        assert info_b.remaining == 4

    def test_single_instance_enforces_limit(self):
        import pytest
        from backtestforecast.errors import RateLimitError

        limiter = _make_limiter()
        for _ in range(3):
            limiter.check(bucket="test", actor_key="u", limit=3, window_seconds=60)

        with pytest.raises(RateLimitError):
            limiter.check(bucket="test", actor_key="u", limit=3, window_seconds=60)
