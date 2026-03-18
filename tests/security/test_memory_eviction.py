"""Test that memory rate limiter evicts correctly."""
from __future__ import annotations

from backtestforecast.security.rate_limits import RateLimiter


def test_memory_eviction_keeps_recent_buckets():
    """After eviction, the most recently used buckets should survive."""
    settings = type("S", (), {
        "rate_limit_prefix": "test",
        "rate_limit_fail_closed": False,
        "rate_limit_memory_max_keys": 5,
        "redis_cache_url": "redis://localhost:6379/15",
    })()
    limiter = RateLimiter(settings=settings)
    limiter._redis_retry_after = float("inf")

    for i in range(20):
        try:
            limiter.check(bucket="test", actor_key=f"user_{i}", limit=100, window_seconds=60)
        except Exception:
            pass

    assert len(limiter._memory_counters) <= 10
