"""Test that the rate limiter in-memory fallback never evicts current-bucket entries.

The RateLimiter._check_memory method has a three-tier eviction strategy
when the number of tracked keys exceeds rate_limit_memory_max_keys:

1. Remove entries older than current_bucket - 2
2. (Aggressive) Remove entries older than current_bucket - 1
3. (Hard cap) Keep only current-bucket entries, filling remaining capacity
   with the most recent older entries

This test suite verifies that entries belonging to the current time bucket
are NEVER evicted, regardless of how many keys exist, and their counts
remain accurate.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_limiter(max_keys: int = 3):
    """Create a RateLimiter with mocked settings and no Redis."""
    from backtestforecast.security.rate_limits import RateLimiter

    settings = MagicMock()
    settings.rate_limit_fail_closed = True
    settings.rate_limit_memory_max_keys = max_keys
    settings.redis_cache_url = "redis://localhost:6379"
    settings.rate_limit_prefix = "test"

    limiter = RateLimiter(settings=settings)
    limiter._redis = None
    limiter._redis_retry_after = float("inf")
    return limiter


class TestEvictionPreservesCurrentBucket:
    def test_current_bucket_entries_survive_hard_cap_eviction(self):
        """When len > max_keys * 2 and all eviction passes run, entries
        whose bucket value equals the current bucket must survive with
        correct counts."""
        limiter = _make_limiter(max_keys=3)

        fixed_time = 1_000_000.0
        window_seconds = 60
        current_bucket = int(fixed_time // window_seconds)

        counters: dict[str, tuple[int, int]] = {}
        for i in range(5):
            counters[f"old_{i}"] = (current_bucket - 1, i + 1)
        counters["current_target"] = (current_bucket, 42)
        counters["current_other_1"] = (current_bucket, 7)
        counters["current_other_2"] = (current_bucket, 13)
        limiter._memory_counters = counters

        with patch("time.time", return_value=fixed_time):
            count, bucket = limiter._check_memory("new_key", window_seconds)

        assert bucket == current_bucket
        assert count == 1

        assert limiter._memory_counters["current_target"] == (current_bucket, 42)
        assert limiter._memory_counters["current_other_1"] == (current_bucket, 7)
        assert limiter._memory_counters["current_other_2"] == (current_bucket, 13)

        new_key = f"new_key:{current_bucket}"
        assert limiter._memory_counters[new_key] == (current_bucket, 1)

        for i in range(5):
            assert f"old_{i}" not in limiter._memory_counters

    def test_current_bucket_count_increments_after_eviction(self):
        """Calling _check_memory for an existing current-bucket key after
        eviction must increment (not reset) its counter."""
        limiter = _make_limiter(max_keys=3)

        fixed_time = 1_000_000.0
        window_seconds = 60
        current_bucket = int(fixed_time // window_seconds)
        target_key_full = f"target:{current_bucket}"

        counters: dict[str, tuple[int, int]] = {}
        for i in range(5):
            counters[f"old_{i}"] = (current_bucket - 1, 1)
        counters[target_key_full] = (current_bucket, 10)
        counters["filler_a"] = (current_bucket, 3)
        counters["filler_b"] = (current_bucket, 5)
        limiter._memory_counters = counters

        with patch("time.time", return_value=fixed_time):
            count, _bucket = limiter._check_memory("target", window_seconds)

        assert count == 11, "Should be 10 + 1 (incremented, not reset)"
        assert limiter._memory_counters[target_key_full] == (current_bucket, 11)

    def test_stale_entries_are_evicted_before_max_keys_doubled(self):
        """Entries older than current_bucket - 2 are evicted in the first pass
        when len > max_keys (but not yet > max_keys * 2)."""
        limiter = _make_limiter(max_keys=3)

        fixed_time = 1_000_000.0
        window_seconds = 60
        current_bucket = int(fixed_time // window_seconds)

        counters: dict[str, tuple[int, int]] = {}
        for i in range(4):
            counters[f"very_old_{i}"] = (current_bucket - 5, 1)
        limiter._memory_counters = counters

        with patch("time.time", return_value=fixed_time):
            count, _bucket = limiter._check_memory("fresh", window_seconds)

        assert count == 1
        for i in range(4):
            assert f"very_old_{i}" not in limiter._memory_counters

    def test_many_current_bucket_keys_all_survive(self):
        """Even if the number of current-bucket entries exceeds max_keys,
        all must survive the hard-cap eviction."""
        limiter = _make_limiter(max_keys=2)

        fixed_time = 1_000_000.0
        window_seconds = 60
        current_bucket = int(fixed_time // window_seconds)

        counters: dict[str, tuple[int, int]] = {}
        for i in range(3):
            counters[f"old_{i}"] = (current_bucket - 1, 1)
        expected_counts = {}
        for i in range(5):
            key = f"cur_{i}"
            counters[key] = (current_bucket, (i + 1) * 10)
            expected_counts[key] = (i + 1) * 10
        limiter._memory_counters = counters

        with patch("time.time", return_value=fixed_time):
            limiter._check_memory("trigger", window_seconds)

        for key, expected in expected_counts.items():
            assert key in limiter._memory_counters, f"{key} was evicted"
            assert limiter._memory_counters[key] == (current_bucket, expected)
