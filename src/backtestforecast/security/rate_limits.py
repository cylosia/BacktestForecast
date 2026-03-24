from __future__ import annotations

import asyncio
import contextlib
import random
import time
from dataclasses import dataclass
from threading import Lock

import structlog
from redis import Redis
from redis.exceptions import RedisError

from backtestforecast.config import Settings, get_settings
from backtestforecast.errors import RateLimitError, ServiceUnavailableError
from backtestforecast.observability.metrics import (
    RATE_LIMIT_HITS_TOTAL,
    REDIS_CONNECTION_ERRORS_TOTAL,
    REDIS_RATE_LIMIT_FALLBACK_TOTAL,
)

logger = structlog.get_logger("security.rate_limits")


@dataclass(frozen=True, slots=True)
class RateLimitInfo:
    limit: int
    remaining: int
    reset_at: int


_RATE_LIMIT_LUA = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return count
"""


class RateLimiter:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._fail_closed = self.settings.rate_limit_fail_closed
        self._memory_lock = Lock()
        self._redis_lock = Lock()
        self._memory_counters: dict[str, tuple[int, int]] = {}
        self._redis: Redis | None = None
        self._redis_retry_after: float = 0.0
        self._lua_sha: str | None = None

    def get_redis(self) -> Redis | None:
        """Return the underlying Redis client, or None if unavailable.

        Public so that other subsystems (e.g. billing circuit breaker) can
        perform lightweight Redis checks without duplicating connection logic.
        """
        return self._get_redis()

    def _get_redis(self) -> Redis | None:
        if self._redis is not None:
            return self._redis
        if time.monotonic() < self._redis_retry_after:
            return None
        with self._redis_lock:
            if self._redis is not None:
                return self._redis
            if time.monotonic() < self._redis_retry_after:
                return None
            try:
                self._redis = Redis.from_url(
                    self.settings.redis_cache_url,
                    decode_responses=True,
                    socket_timeout=2.0,
                    socket_connect_timeout=2.0,
                )
                self._redis.ping()
            except Exception:
                self._redis = None
                self._redis_retry_after = time.monotonic() + 30.0 + random.uniform(0, 10)
        return self._redis

    def check(self, *, bucket: str, actor_key: str, limit: int, window_seconds: int) -> RateLimitInfo:
        if limit <= 0:
            return RateLimitInfo(limit=0, remaining=0, reset_at=0)
        namespaced = f"{self.settings.rate_limit_prefix}:{bucket}:{actor_key}"
        redis = self._get_redis()
        count: int | None = None
        current_bucket = int(time.time() // window_seconds)
        degraded_memory_fallback = bool(
            getattr(self.settings, "rate_limit_degraded_memory_fallback", False)
        )
        if redis is None and self._fail_closed:
            logger.error(
                "rate_limiter.fail_closed_redis_unavailable",
                bucket=bucket,
                msg="Redis unavailable while fail-closed mode is enabled; rejecting request.",
            )
            raise ServiceUnavailableError()
        try:
            if redis is not None:
                count, current_bucket = self._check_redis(namespaced, window_seconds)
        except RedisError:
            with self._redis_lock:
                self._redis = None
                self._redis_retry_after = time.monotonic() + 30.0 + random.uniform(0, 10)
            REDIS_CONNECTION_ERRORS_TOTAL.labels(operation="rate_limit").inc()
            REDIS_RATE_LIMIT_FALLBACK_TOTAL.labels(bucket=bucket).inc()
            logger.warning("rate_limiter.redis_fallback", key=bucket, exc_info=True)
            if self._fail_closed:
                logger.error(
                    "rate_limiter.fail_closed_redis_error",
                    bucket=bucket,
                    msg="Redis rate-limit operation failed while fail-closed mode is enabled; rejecting request.",
                )
                raise ServiceUnavailableError() from None
            if degraded_memory_fallback:
                fallback_limit = max(limit // 2, 1)
                mem_count, _ = self._check_memory(namespaced, window_seconds)
                logger.warning(
                    "rate_limiter.degraded_memory_fallback",
                    bucket=bucket,
                    mem_count=mem_count,
                    fallback_limit=fallback_limit,
                    msg="Redis unavailable; using degraded in-memory rate limiting fallback. "
                    "Limits are halved and per-process only (not shared across workers).",
                )
                count = mem_count
                limit = fallback_limit
        if count is None:
            mem_count, current_bucket = self._check_memory(namespaced, window_seconds)
            count = mem_count
        remaining = max(limit - count, 0)
        reset_at = (current_bucket + 1) * window_seconds
        info = RateLimitInfo(limit=limit, remaining=remaining, reset_at=reset_at)
        if count > limit:
            RATE_LIMIT_HITS_TOTAL.labels(bucket=bucket).inc()
            err = RateLimitError()
            err.rate_limit_info = info
            raise err
        _NEAR_LIMIT_THRESHOLD = 0.8
        if limit >= 5 and count >= int(limit * _NEAR_LIMIT_THRESHOLD):
            from backtestforecast.observability.metrics import RATE_LIMIT_NEAR_TOTAL
            RATE_LIMIT_NEAR_TOTAL.labels(bucket=bucket).inc()
        return info

    async def async_check(
        self, *, bucket: str, actor_key: str, limit: int, window_seconds: int,
    ) -> RateLimitInfo:
        """Async wrapper for use in async handlers or middleware."""
        return await asyncio.to_thread(
            self.check, bucket=bucket, actor_key=actor_key, limit=limit, window_seconds=window_seconds,
        )

    def ping(self) -> bool:
        try:
            redis = self._get_redis()
            if redis is None:
                return False
            return bool(redis.ping())
        except RedisError:
            with self._redis_lock:
                self._redis = None
                self._redis_retry_after = time.monotonic() + 30.0 + random.uniform(0, 10)
            REDIS_CONNECTION_ERRORS_TOTAL.labels(operation="ping").inc()
            return False

    def get_pool_stats(self) -> dict[str, int]:
        """Return Redis connection pool statistics for monitoring."""
        redis = self._get_redis()
        if redis is None:
            return {"status": "disconnected"}
        try:
            pool = redis.connection_pool
            return {
                "status": "connected",
                "max_connections": getattr(pool, "max_connections", -1),
                "current_connections": len(getattr(pool, "_available_connections", [])) + len(getattr(pool, "_in_use_connections", [])),
            }
        except Exception:
            return {"status": "error"}

    def close(self) -> None:
        """Release Redis connection resources."""
        with self._redis_lock:
            redis = self._redis
            self._redis = None
        if redis is not None:
            with contextlib.suppress(Exception):
                redis.close()

    def reset(self) -> None:
        with self._memory_lock:
            self._memory_counters.clear()

    def _check_redis(self, key: str, window_seconds: int) -> tuple[int, int]:
        bucket = int(time.time() // window_seconds)
        bucket_key = f"{key}:{bucket}"
        redis = self._redis
        if redis is None:
            raise RedisError("Redis client is not initialized")
        with self._memory_lock:
            sha = self._lua_sha
        if sha is None:
            sha = redis.script_load(_RATE_LIMIT_LUA)
            with self._memory_lock:
                self._lua_sha = sha
        try:
            count = redis.evalsha(sha, 1, bucket_key, window_seconds + 10)
        except RedisError:
            sha = redis.script_load(_RATE_LIMIT_LUA)
            with self._memory_lock:
                self._lua_sha = sha
            count = redis.evalsha(sha, 1, bucket_key, window_seconds + 10)
        return int(count), bucket

    def _check_memory(self, key: str, window_seconds: int) -> tuple[int, int]:
        bucket = int(time.time() // window_seconds)
        namespaced = f"{key}:{bucket}"
        max_keys = self.settings.rate_limit_memory_max_keys
        hard_cap = max_keys * 2
        with self._memory_lock:
            if len(self._memory_counters) > max_keys:
                cutoff = bucket - 2
                stale = [k for k, (b, _) in self._memory_counters.items() if b < cutoff]
                for k in stale:
                    del self._memory_counters[k]
            if len(self._memory_counters) > hard_cap:
                logger.warning("rate_limiter.memory_hard_cap", size=len(self._memory_counters), max_keys=max_keys)
                cutoff_aggressive = bucket - 1
                stale_aggressive = [k for k, (b, _) in self._memory_counters.items() if b < cutoff_aggressive]
                for k in stale_aggressive:
                    del self._memory_counters[k]
                if len(self._memory_counters) > hard_cap:
                    current_items = {k: v for k, v in self._memory_counters.items() if v[0] >= bucket}
                    older_items = {k: v for k, v in self._memory_counters.items() if v[0] < bucket}
                    keep_older = max(0, max_keys - len(current_items))
                    if keep_older > 0 and older_items:
                        sorted_older = sorted(older_items.items(), key=lambda kv: kv[1][0], reverse=True)
                        current_items.update(dict(sorted_older[:keep_older]))
                    self._memory_counters = current_items
            counter_bucket, counter_value = self._memory_counters.get(namespaced, (bucket, 0))
            if counter_bucket != bucket:
                counter_value = 0
            counter_value += 1
            self._memory_counters[namespaced] = (bucket, counter_value)
            if len(self._memory_counters) > hard_cap:
                overflow = len(self._memory_counters) - hard_cap
                if overflow > 0:
                    # Enforce a true upper bound even when all keys are in the
                    # current bucket and age-based eviction cannot help.
                    oldest_keys = list(self._memory_counters.keys())[:overflow]
                    for old_key in oldest_keys:
                        if old_key != namespaced:
                            del self._memory_counters[old_key]
                    if len(self._memory_counters) > hard_cap:
                        self._memory_counters = dict(list(self._memory_counters.items())[-hard_cap:])
            return counter_value, bucket


_rate_limiter: RateLimiter | None = None
_rate_limiter_lock = Lock()


def get_rate_limiter() -> RateLimiter:
    global _rate_limiter
    if _rate_limiter is not None:
        return _rate_limiter
    with _rate_limiter_lock:
        if _rate_limiter is not None:
            return _rate_limiter
        _rate_limiter = RateLimiter()
        return _rate_limiter


def ping_redis() -> bool:
    return get_rate_limiter().ping()


def _invalidate_rate_limiter() -> None:
    """Close and discard the cached rate limiter so the next call
    to ``get_rate_limiter()`` creates a fresh instance with updated settings."""
    global _rate_limiter
    with _rate_limiter_lock:
        old = _rate_limiter
        _rate_limiter = None
    if old is not None:
        old.close()


from backtestforecast.config import register_invalidation_callback as _register  # noqa: E402

_register(_invalidate_rate_limiter)
