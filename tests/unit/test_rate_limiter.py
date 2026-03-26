"""Tests for rate limiter edge cases."""
from __future__ import annotations

import pytest

from backtestforecast.errors import RateLimitError
from backtestforecast.security.rate_limits import RateLimiter


class TestRateLimiterMemoryFallback:
    def test_memory_counter_increments(self):
        limiter = RateLimiter.__new__(RateLimiter)
        from threading import Lock

        from backtestforecast.config import get_settings
        limiter.settings = get_settings()
        limiter._memory_lock = Lock()
        limiter._memory_counters = {}

        count, _bucket = limiter._check_memory("test:key", 60)
        assert count == 1

        count2, _ = limiter._check_memory("test:key", 60)
        assert count2 == 2

    def test_rate_limit_exceeded_raises(self):
        from threading import Lock

        from backtestforecast.config import get_settings

        limiter = RateLimiter.__new__(RateLimiter)
        limiter.settings = get_settings()
        limiter._fail_closed = False
        limiter._memory_lock = Lock()
        limiter._redis_lock = Lock()
        limiter._memory_counters = {}
        limiter._redis = None
        limiter._redis_retry_after = float("inf")
        limiter._lua_sha = None

        for _ in range(5):
            limiter.check(bucket="test", actor_key="u1", limit=5, window_seconds=60)

        with pytest.raises(RateLimitError):
            limiter.check(bucket="test", actor_key="u1", limit=5, window_seconds=60)

    def test_different_actors_independent(self):
        from threading import Lock

        from backtestforecast.config import get_settings

        limiter = RateLimiter.__new__(RateLimiter)
        limiter.settings = get_settings()
        limiter._fail_closed = False
        limiter._memory_lock = Lock()
        limiter._redis_lock = Lock()
        limiter._memory_counters = {}
        limiter._redis = None
        limiter._redis_retry_after = float("inf")
        limiter._lua_sha = None

        for _ in range(3):
            limiter.check(bucket="test", actor_key="u1", limit=5, window_seconds=60)
            limiter.check(bucket="test", actor_key="u2", limit=5, window_seconds=60)

        info1 = limiter.check(bucket="test", actor_key="u1", limit=5, window_seconds=60)
        assert info1.remaining == 1

        info2 = limiter.check(bucket="test", actor_key="u2", limit=5, window_seconds=60)
        assert info2.remaining == 1
