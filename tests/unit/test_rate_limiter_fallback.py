"""Test rate limiter fallback behavior when Redis is unavailable."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest
from redis.exceptions import RedisError

from backtestforecast.config import Settings
from backtestforecast.errors import RateLimitError, ServiceUnavailableError
from backtestforecast.security.rate_limits import RateLimiter, RateLimitInfo


def _make_settings(**overrides) -> Settings:
    defaults = {
        "redis_cache_url": "redis://localhost:6379/0",
        "rate_limit_fail_closed": False,
        "rate_limit_degraded_memory_fallback": False,
        "rate_limit_prefix": "test:rl",
        "rate_limit_memory_max_keys": 100,
        "feature_backtests_enabled": False,
        "feature_scanner_enabled": False,
        "feature_sweeps_enabled": False,
        "feature_analysis_enabled": False,
    }
    defaults.update(overrides)
    return Settings(**defaults)


class TestInMemoryFallbackWhenRedisDown:
    """When Redis is unavailable, RateLimiter falls back to in-memory counters."""

    def test_check_succeeds_via_memory_when_redis_is_none(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999

        info = limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        assert isinstance(info, RateLimitInfo)
        assert info.remaining == 9
        assert info.limit == 10

    def test_memory_counter_increments(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999

        info1 = limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        info2 = limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        info3 = limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        assert info1.remaining == 9
        assert info2.remaining == 8
        assert info3.remaining == 7

    def test_redis_error_triggers_fallback(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        mock_redis = MagicMock()
        limiter._redis = mock_redis
        limiter._lua_sha = "fakeSHA"
        mock_redis.evalsha.side_effect = RedisError("Connection lost")
        mock_redis.script_load.side_effect = RedisError("Connection lost")

        info = limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        assert isinstance(info, RateLimitInfo)
        assert info.remaining == 9
        assert limiter._redis is None

    def test_redis_error_sets_retry_delay(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        mock_redis = MagicMock()
        limiter._redis = mock_redis
        limiter._lua_sha = "fakeSHA"
        mock_redis.evalsha.side_effect = RedisError("Connection lost")
        mock_redis.script_load.side_effect = RedisError("Connection lost")

        before = time.monotonic()
        limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        assert limiter._redis_retry_after > before + 29

    def test_exceeding_limit_raises_rate_limit_error(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999

        for _ in range(3):
            limiter.check(bucket="api", actor_key="user1", limit=3, window_seconds=60)

        with pytest.raises(RateLimitError):
            limiter.check(bucket="api", actor_key="user1", limit=3, window_seconds=60)

    def test_rate_limit_error_contains_info(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999

        for _ in range(2):
            limiter.check(bucket="api", actor_key="user1", limit=2, window_seconds=60)

        with pytest.raises(RateLimitError) as exc_info:
            limiter.check(bucket="api", actor_key="user1", limit=2, window_seconds=60)
        assert exc_info.value.rate_limit_info.limit == 2
        assert exc_info.value.rate_limit_info.remaining == 0

    def test_different_actors_have_separate_counters(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999

        limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        info_user2 = limiter.check(bucket="api", actor_key="user2", limit=10, window_seconds=60)
        assert info_user2.remaining == 9

    def test_different_buckets_have_separate_counters(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999

        limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        info_export = limiter.check(bucket="export", actor_key="user1", limit=10, window_seconds=60)
        assert info_export.remaining == 9

    def test_zero_limit_returns_zero_remaining(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        info = limiter.check(bucket="api", actor_key="user1", limit=0, window_seconds=60)
        assert info.remaining == 0
        assert info.limit == 0


class TestFailClosedMode:
    """When fail_closed=True and Redis is down, requests are rejected."""

    def test_fail_closed_raises_service_unavailable(self):
        settings = _make_settings(rate_limit_fail_closed=True)
        limiter = RateLimiter(settings=settings)
        mock_redis = MagicMock()
        limiter._redis = mock_redis
        limiter._lua_sha = "fakeSHA"
        mock_redis.evalsha.side_effect = RedisError("Connection lost")
        mock_redis.script_load.side_effect = RedisError("Connection lost")

        with pytest.raises(ServiceUnavailableError):
            limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)

    def test_fail_closed_raises_when_redis_is_unavailable_before_operation(self):
        settings = _make_settings(rate_limit_fail_closed=True)
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999

        with pytest.raises(ServiceUnavailableError):
            limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)

    def test_fail_open_succeeds_when_redis_down(self):
        settings = _make_settings(rate_limit_fail_closed=False)
        limiter = RateLimiter(settings=settings)
        mock_redis = MagicMock()
        limiter._redis = mock_redis
        limiter._lua_sha = "fakeSHA"
        mock_redis.evalsha.side_effect = RedisError("Connection lost")
        mock_redis.script_load.side_effect = RedisError("Connection lost")

        info = limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        assert info.remaining == 9

    def test_degraded_memory_fallback_halves_effective_limit(self):
        settings = _make_settings(
            rate_limit_fail_closed=False,
            rate_limit_degraded_memory_fallback=True,
        )
        limiter = RateLimiter(settings=settings)
        mock_redis = MagicMock()
        limiter._redis = mock_redis
        limiter._lua_sha = "fakeSHA"
        mock_redis.evalsha.side_effect = RedisError("Connection lost")
        mock_redis.script_load.side_effect = RedisError("Connection lost")
        limiter._get_redis = lambda: mock_redis  # type: ignore[method-assign]

        for _ in range(5):
            limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)

        with pytest.raises(RateLimitError) as exc_info:
            limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)

        assert exc_info.value.rate_limit_info.limit == 5


class TestMemoryCapEviction:
    """The in-memory counter map evicts stale keys when exceeding max_keys."""

    def test_eviction_caps_memory_counters(self):
        settings = _make_settings(rate_limit_memory_max_keys=5)
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999

        for i in range(20):
            limiter.check(
                bucket="api",
                actor_key=f"user{i}",
                limit=100,
                window_seconds=60,
            )

        # The implementation bounds growth by evicting stale keys in batches
        # once the configured ceiling is exceeded.
        assert len(limiter._memory_counters) <= 10

    def test_reset_clears_counters(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999

        limiter.check(bucket="api", actor_key="user1", limit=10, window_seconds=60)
        assert len(limiter._memory_counters) > 0

        limiter.reset()
        assert len(limiter._memory_counters) == 0

    def test_counters_survive_below_max(self):
        settings = _make_settings(rate_limit_memory_max_keys=100)
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999

        for i in range(5):
            limiter.check(bucket="api", actor_key=f"user{i}", limit=100, window_seconds=60)

        assert len(limiter._memory_counters) == 5


class TestCheckMemoryDirectly:
    """Test the _check_memory method directly."""

    def test_returns_count_and_bucket(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        count, bucket = limiter._check_memory("test:key:actor", 60)
        assert count == 1
        assert isinstance(bucket, int)

    def test_increments_on_repeated_calls(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        c1, _ = limiter._check_memory("test:key:actor", 60)
        c2, _ = limiter._check_memory("test:key:actor", 60)
        c3, _ = limiter._check_memory("test:key:actor", 60)
        assert c1 == 1
        assert c2 == 2
        assert c3 == 3

    def test_different_keys_independent(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        c_a, _ = limiter._check_memory("key:a", 60)
        c_b, _ = limiter._check_memory("key:b", 60)
        assert c_a == 1
        assert c_b == 1

    def test_bucket_is_time_based(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        _, bucket = limiter._check_memory("test:key", 60)
        expected_bucket = int(time.time() // 60)
        assert bucket == expected_bucket


class TestRateLimitInfoFields:
    """Verify the RateLimitInfo dataclass returned by check()."""

    def test_info_has_correct_fields(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999

        info = limiter.check(bucket="api", actor_key="user1", limit=5, window_seconds=120)
        assert info.limit == 5
        assert info.remaining == 4
        assert info.reset_at > 0

    def test_reset_at_is_next_window(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999

        window = 120
        info = limiter.check(bucket="api", actor_key="user1", limit=5, window_seconds=window)
        current_bucket = int(time.time() // window)
        expected_reset = (current_bucket + 1) * window
        assert info.reset_at == expected_reset

    def test_info_is_frozen(self):
        info = RateLimitInfo(limit=10, remaining=5, reset_at=1000)
        with pytest.raises(AttributeError):
            info.limit = 20


class TestPingMethod:
    """Test the ping() method for health checks."""

    def test_ping_returns_false_when_redis_none(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        limiter._redis = None
        limiter._redis_retry_after = time.monotonic() + 9999
        assert limiter.ping() is False

    def test_ping_returns_true_when_redis_responds(self):
        settings = _make_settings()
        limiter = RateLimiter(settings=settings)
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        limiter._redis = mock_redis
        assert limiter.ping() is True
