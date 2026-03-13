from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from threading import Lock

import structlog
from redis import Redis
from redis.exceptions import RedisError

from backtestforecast.config import Settings, get_settings
from backtestforecast.errors import AppError, RateLimitError
from backtestforecast.observability.metrics import RATE_LIMIT_HITS_TOTAL, REDIS_RATE_LIMIT_FALLBACK_TOTAL

logger = structlog.get_logger("security.rate_limits")


@dataclass(frozen=True, slots=True)
class RateLimitInfo:
    limit: int
    remaining: int
    reset_at: int


class ServiceUnavailableError(AppError):
    def __init__(self, message: str = "Service temporarily unavailable. Please retry later.") -> None:
        super().__init__(code="service_unavailable", message=message, status_code=503)


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
        self._memory_counters: dict[str, tuple[int, int]] = {}
        self._redis: Redis | None = None
        self._redis_retry_after: float = 0.0
        self._lua_sha: str | None = None

    def _get_redis(self) -> Redis | None:
        if self._redis is not None:
            return self._redis
        if time.time() < self._redis_retry_after:
            return None
        try:
            self._redis = Redis.from_url(
                self.settings.redis_url,
                decode_responses=True,
                socket_timeout=2.0,
                socket_connect_timeout=2.0,
            )
            self._redis.ping()
        except Exception:
            self._redis = None
            self._redis_retry_after = time.time() + 30.0
        return self._redis

    def check(self, *, bucket: str, actor_key: str, limit: int, window_seconds: int) -> RateLimitInfo:
        if limit <= 0:
            return RateLimitInfo(limit=0, remaining=0, reset_at=0)
        namespaced = f"{self.settings.rate_limit_prefix}:{bucket}:{actor_key}"
        redis = self._get_redis()
        count: int | None = None
        current_bucket = int(time.time() // window_seconds)
        try:
            if redis is not None:
                count, current_bucket = self._check_redis(namespaced, window_seconds)
        except RedisError:
            REDIS_RATE_LIMIT_FALLBACK_TOTAL.labels(bucket=bucket).inc()
            logger.warning("rate_limiter.redis_fallback", key=bucket, exc_info=True)
            if self._fail_closed:
                logger.error("rate_limiter.fail_closed", bucket=bucket)
                raise ServiceUnavailableError()
        if count is None:
            count, current_bucket = self._check_memory(namespaced, window_seconds)
        remaining = max(limit - count, 0)
        reset_at = (current_bucket + 1) * window_seconds
        info = RateLimitInfo(limit=limit, remaining=remaining, reset_at=reset_at)
        if count > limit:
            RATE_LIMIT_HITS_TOTAL.labels(bucket=bucket).inc()
            err = RateLimitError()
            err.rate_limit_info = info  # type: ignore[attr-defined]
            raise err
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
            return False

    def reset(self) -> None:
        with self._memory_lock:
            self._memory_counters.clear()

    def _check_redis(self, key: str, window_seconds: int) -> tuple[int, int]:
        bucket = int(time.time() // window_seconds)
        bucket_key = f"{key}:{bucket}"
        redis = self._redis
        assert redis is not None
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
        with self._memory_lock:
            if len(self._memory_counters) > self.settings.rate_limit_memory_max_keys:
                cutoff = bucket - 2
                stale = [k for k, (b, _) in self._memory_counters.items() if b < cutoff]
                for k in stale:
                    del self._memory_counters[k]
            counter_bucket, counter_value = self._memory_counters.get(namespaced, (bucket, 0))
            if counter_bucket != bucket:
                counter_value = 0
            counter_value += 1
            self._memory_counters[namespaced] = (bucket, counter_value)
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
