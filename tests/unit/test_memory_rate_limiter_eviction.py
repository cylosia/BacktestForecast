"""Tests for memory rate limiter eviction accuracy."""
from __future__ import annotations

import time
from threading import Lock

from backtestforecast.config import get_settings
from backtestforecast.security.rate_limits import RateLimiter


def _make_limiter(max_keys: int = 10) -> RateLimiter:
    """Create a RateLimiter wired to use in-memory counters only."""
    limiter = RateLimiter.__new__(RateLimiter)
    settings = get_settings()
    limiter.settings = settings
    limiter._fail_closed = False
    limiter._memory_lock = Lock()
    limiter._redis_lock = Lock()
    limiter._memory_counters = {}
    limiter._redis = None
    limiter._redis_retry_after = 0.0
    limiter._lua_sha = None
    limiter.settings = settings.model_copy(
        update={"rate_limit_memory_max_keys": max_keys},
    )
    return limiter


class TestSoftEviction:
    """When len(_memory_counters) > max_keys, stale entries (bucket < current-2)
    are removed."""

    def test_stale_entries_evicted_on_soft_threshold(self):
        limiter = _make_limiter(max_keys=5)
        window = 60
        current_bucket = int(time.time() // window)
        old_bucket = current_bucket - 3

        for i in range(6):
            limiter._memory_counters[f"stale:{i}:{old_bucket}"] = (old_bucket, 1)

        count, _ = limiter._check_memory("fresh:key", window)
        assert count == 1
        assert len(limiter._memory_counters) <= 5 + 1

    def test_recent_entries_survive_soft_eviction(self):
        limiter = _make_limiter(max_keys=5)
        window = 60
        current_bucket = int(time.time() // window)

        for i in range(6):
            limiter._memory_counters[f"recent:{i}:{current_bucket}"] = (current_bucket, 1)

        count, _ = limiter._check_memory("fresh:key", window)
        assert count == 1
        assert f"fresh:key:{current_bucket}" in limiter._memory_counters


class TestAggressiveEviction:
    """When len(_memory_counters) > max_keys * 2 after soft eviction, the hard
    cap aggressively removes stale/older buckets without evicting current-bucket actors."""

    def test_hard_cap_preserves_current_bucket_entries(self):
        limiter = _make_limiter(max_keys=5)
        window = 60
        current_bucket = int(time.time() // window)

        for i in range(15):
            limiter._memory_counters[f"key:{i}:{current_bucket}"] = (current_bucket, 1)

        count, _ = limiter._check_memory("newest:key", window)
        assert count == 1
        assert len(limiter._memory_counters) == 16
        assert f"newest:key:{current_bucket}" in limiter._memory_counters

    def test_most_recent_actor_preserved_after_hard_eviction(self):
        limiter = _make_limiter(max_keys=5)
        window = 60
        current_bucket = int(time.time() // window)

        for i in range(20):
            limiter._memory_counters[f"key:{i}:{current_bucket}"] = (current_bucket, i)

        count, _ = limiter._check_memory("target:key", window)
        assert count == 1

        target_namespaced = f"target:key:{current_bucket}"
        assert target_namespaced in limiter._memory_counters
        assert limiter._memory_counters[target_namespaced] == (current_bucket, 1)

    def test_eviction_preserves_highest_bucket_entries(self):
        limiter = _make_limiter(max_keys=5)
        window = 60
        current_bucket = int(time.time() // window)
        older_bucket = current_bucket - 1

        for i in range(6):
            limiter._memory_counters[f"old:{i}:{older_bucket}"] = (older_bucket, 1)
        for i in range(6):
            limiter._memory_counters[f"new:{i}:{current_bucket}"] = (current_bucket, 1)

        limiter._check_memory("probe:key", window)

        remaining_buckets = {v[0] for v in limiter._memory_counters.values()}
        assert current_bucket in remaining_buckets


class TestCrossBucketReset:
    """_check_memory resets the counter when the stored bucket doesn't match
    the current time bucket."""

    def test_counter_resets_when_bucket_changes(self):
        limiter = _make_limiter(max_keys=100)
        window = 60
        current_bucket = int(time.time() // window)
        old_bucket = current_bucket - 1

        namespaced = f"test:actor:{current_bucket}"
        limiter._memory_counters[namespaced] = (old_bucket, 99)

        count, _ = limiter._check_memory("test:actor", window)
        assert count == 1, "Counter should reset to 1 when bucket changes"

    def test_counter_accumulates_within_same_bucket(self):
        limiter = _make_limiter(max_keys=100)
        window = 60

        c1, _ = limiter._check_memory("actor:same", window)
        c2, _ = limiter._check_memory("actor:same", window)
        c3, _ = limiter._check_memory("actor:same", window)

        assert c1 == 1
        assert c2 == 2
        assert c3 == 3


class TestFullCheckFallback:
    """The public .check() method falls back to _check_memory when Redis is
    unavailable."""

    def test_check_uses_memory_when_redis_is_none(self):
        limiter = _make_limiter(max_keys=10)

        info = limiter.check(bucket="test", actor_key="user1", limit=100, window_seconds=60)
        assert info.remaining == 99
        assert info.limit == 100

        info2 = limiter.check(bucket="test", actor_key="user1", limit=100, window_seconds=60)
        assert info2.remaining == 98

    def test_many_actors_trigger_eviction_via_check(self):
        limiter = _make_limiter(max_keys=10)

        for i in range(25):
            limiter.check(bucket="test", actor_key=f"actor_{i}", limit=100, window_seconds=60)

        final = limiter.check(bucket="test", actor_key="final_actor", limit=100, window_seconds=60)
        assert final.remaining == 99
        current_bucket = int(time.time() // 60)
        assert len(limiter._memory_counters) == 26
        assert f"bff:rate-limit:test:final_actor:{current_bucket}" in limiter._memory_counters
