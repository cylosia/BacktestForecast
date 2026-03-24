"""Unit tests for the RateLimiter class.

All tests use the in-memory fallback path (no Redis required) by patching
``_get_redis`` to return ``None``.  This keeps tests fast, deterministic,
and dependency-free.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from redis.exceptions import RedisError

from backtestforecast.errors import RateLimitError, ServiceUnavailableError
from backtestforecast.security.rate_limits import (
    RateLimiter,
    RateLimitInfo,
)

_CHECK = dict(bucket="test", actor_key="user-1", limit=5, window_seconds=60)


def _make_limiter(*, fail_closed: bool = False, memory_max_keys: int = 10_000) -> RateLimiter:
    settings = MagicMock()
    settings.rate_limit_prefix = "test"
    settings.rate_limit_fail_closed = fail_closed
    settings.rate_limit_degraded_memory_fallback = False
    settings.rate_limit_memory_max_keys = memory_max_keys
    settings.redis_url = "redis://localhost:6379/0"
    limiter = RateLimiter(settings=settings)
    limiter._get_redis = lambda: None  # type: ignore[assignment]
    return limiter


def test_under_limit_returns_correct_remaining():
    limiter = _make_limiter()
    info = limiter.check(**_CHECK)
    assert isinstance(info, RateLimitInfo)
    assert info.remaining == 4
    assert info.limit == 5


def test_exceeding_limit_raises_rate_limit_error():
    limiter = _make_limiter()
    for _ in range(5):
        limiter.check(**_CHECK)

    with pytest.raises(RateLimitError) as exc_info:
        limiter.check(**_CHECK)

    assert exc_info.value.status_code == 429
    info = exc_info.value.rate_limit_info  # type: ignore[attr-defined]
    assert info.remaining == 0


def test_different_buckets_are_independent():
    limiter = _make_limiter()
    for _ in range(5):
        limiter.check(bucket="bucket_a", actor_key="u1", limit=5, window_seconds=60)

    with pytest.raises(RateLimitError):
        limiter.check(bucket="bucket_a", actor_key="u1", limit=5, window_seconds=60)

    info = limiter.check(bucket="bucket_b", actor_key="u1", limit=5, window_seconds=60)
    assert info.remaining == 4


def test_different_actors_are_independent():
    limiter = _make_limiter()
    for _ in range(5):
        limiter.check(bucket="b", actor_key="alice", limit=5, window_seconds=60)

    with pytest.raises(RateLimitError):
        limiter.check(bucket="b", actor_key="alice", limit=5, window_seconds=60)

    info = limiter.check(bucket="b", actor_key="bob", limit=5, window_seconds=60)
    assert info.remaining == 4


def test_reset_clears_counters():
    limiter = _make_limiter()
    for _ in range(4):
        limiter.check(**_CHECK)

    limiter.reset()

    info = limiter.check(**_CHECK)
    assert info.remaining == 4


def test_zero_limit_returns_immediately():
    limiter = _make_limiter()
    info = limiter.check(bucket="z", actor_key="u", limit=0, window_seconds=60)
    assert info == RateLimitInfo(limit=0, remaining=0, reset_at=0)


def test_stale_key_eviction():
    limiter = _make_limiter(memory_max_keys=2)
    limiter.check(bucket="a", actor_key="u", limit=100, window_seconds=60)
    limiter.check(bucket="b", actor_key="u", limit=100, window_seconds=60)
    limiter.check(bucket="c", actor_key="u", limit=100, window_seconds=60)

    info = limiter.check(bucket="d", actor_key="u", limit=100, window_seconds=60)
    assert info.remaining >= 1


def test_redis_failure_falls_back_to_memory():
    limiter = _make_limiter()
    mock_redis = MagicMock()
    mock_redis.evalsha.side_effect = RedisError("connection lost")
    mock_redis.script_load.side_effect = RedisError("connection lost")
    limiter._get_redis = lambda: mock_redis  # type: ignore[assignment]
    limiter._redis = mock_redis

    info = limiter.check(**_CHECK)
    assert info.remaining == 4


def test_fail_closed_raises_service_unavailable():
    limiter = _make_limiter(fail_closed=True)
    mock_redis = MagicMock()
    mock_redis.evalsha.side_effect = RedisError("connection lost")
    mock_redis.script_load.side_effect = RedisError("connection lost")
    limiter._get_redis = lambda: mock_redis  # type: ignore[assignment]
    limiter._redis = mock_redis

    with pytest.raises(ServiceUnavailableError) as exc_info:
        limiter.check(**_CHECK)
    assert exc_info.value.status_code == 503


def test_rate_limit_info_fields():
    limiter = _make_limiter()
    info = limiter.check(bucket="f", actor_key="u", limit=10, window_seconds=60)
    assert info.limit == 10
    assert info.remaining == 9
    assert info.reset_at > 0
    assert info.reset_at % 60 == 0


# ---------------------------------------------------------------------------
# Item 70: Rate limiter behavior under high load
# ---------------------------------------------------------------------------


def test_high_load_rejects_after_limit_and_stays_rejected():
    """After reaching the limit, every subsequent request must be rejected
    within the same time window - the counter must not reset unexpectedly."""
    limiter = _make_limiter()
    limit = 5
    params = dict(bucket="highload", actor_key="user-load", limit=limit, window_seconds=60)

    for i in range(limit):
        info = limiter.check(**params)
        assert info.remaining == limit - (i + 1)

    for _ in range(20):
        with pytest.raises(RateLimitError):
            limiter.check(**params)


def test_high_load_different_actors_independent():
    """Under high load, rate limiting for one actor must not affect another."""
    limiter = _make_limiter()
    limit = 3

    for _ in range(limit):
        limiter.check(bucket="load", actor_key="heavy", limit=limit, window_seconds=60)

    with pytest.raises(RateLimitError):
        limiter.check(bucket="load", actor_key="heavy", limit=limit, window_seconds=60)

    info = limiter.check(bucket="load", actor_key="fresh", limit=limit, window_seconds=60)
    assert info.remaining == limit - 1


def test_redis_happy_path_returns_count_from_redis():
    """When Redis works, the count should come from the Lua script result."""
    limiter = _make_limiter()
    mock_redis = MagicMock()
    mock_redis.evalsha.return_value = 3
    mock_redis.script_load.return_value = "fakeshaXYZ"
    limiter._get_redis = lambda: mock_redis  # type: ignore[assignment]
    limiter._redis = mock_redis

    info = limiter.check(bucket="redis_ok", actor_key="u1", limit=10, window_seconds=60)
    assert info.remaining == 7
    assert info.limit == 10
    mock_redis.evalsha.assert_called_once()


def test_high_load_remaining_reaches_zero_at_limit():
    """At exactly the limit count, remaining must be 0 (not negative)."""
    limiter = _make_limiter()
    limit = 10
    params = dict(bucket="exact", actor_key="u", limit=limit, window_seconds=60)

    last_info = None
    for _ in range(limit):
        last_info = limiter.check(**params)

    assert last_info is not None
    assert last_info.remaining == 0

    with pytest.raises(RateLimitError) as exc_info:
        limiter.check(**params)
    assert exc_info.value.rate_limit_info.remaining == 0  # type: ignore[attr-defined]
