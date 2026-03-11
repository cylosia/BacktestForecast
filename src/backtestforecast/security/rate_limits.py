from __future__ import annotations

import time
from threading import Lock

import structlog
from redis import Redis
from redis.exceptions import RedisError

from backtestforecast.config import Settings, get_settings
from backtestforecast.errors import AppError, RateLimitError
from backtestforecast.observability.metrics import RATE_LIMIT_HITS_TOTAL

logger = structlog.get_logger("security.rate_limits")


class ServiceUnavailableError(AppError):
    def __init__(self, message: str = "Service temporarily unavailable. Please retry later.") -> None:
        super().__init__(code="service_unavailable", message=message, status_code=503)


class RateLimiter:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._fail_closed = self.settings.rate_limit_fail_closed
        self._memory_lock = Lock()
        self._memory_counters: dict[str, tuple[int, int]] = {}
        try:
            self._redis = Redis.from_url(self.settings.redis_url, decode_responses=True)
        except Exception:  # pragma: no cover - defensive init fallback
            self._redis = None

    def check(self, *, bucket: str, actor_key: str, limit: int, window_seconds: int) -> None:
        if limit <= 0:
            return
        namespaced = f"{self.settings.rate_limit_prefix}:{bucket}:{actor_key}"
        try:
            if self._redis is not None:
                self._check_redis(namespaced, limit, window_seconds)
                return
        except RateLimitError:
            RATE_LIMIT_HITS_TOTAL.labels(bucket=bucket).inc()
            raise
        except RedisError:
            logger.warning("rate_limiter.redis_fallback", key=bucket, exc_info=True)
            if self._fail_closed:
                logger.error("rate_limiter.fail_closed", bucket=bucket)
                raise ServiceUnavailableError()
        try:
            self._check_memory(namespaced, limit, window_seconds)
        except RateLimitError:
            RATE_LIMIT_HITS_TOTAL.labels(bucket=bucket).inc()
            raise

    def ping(self) -> bool:
        try:
            if self._redis is None:
                return False
            return bool(self._redis.ping())
        except RedisError:
            return False

    def reset(self) -> None:
        with self._memory_lock:
            self._memory_counters.clear()

    def _check_redis(self, key: str, limit: int, window_seconds: int) -> None:
        bucket = int(time.time() // window_seconds)
        bucket_key = f"{key}:{bucket}"
        count = self._redis.incr(bucket_key)  # type: ignore[union-attr]
        if count == 1:
            self._redis.expire(bucket_key, window_seconds * 2)  # type: ignore[union-attr]
        if int(count) > limit:
            raise RateLimitError()

    def _check_memory(self, key: str, limit: int, window_seconds: int) -> None:
        bucket = int(time.time() // window_seconds)
        namespaced = f"{key}:{bucket}"
        with self._memory_lock:
            if len(self._memory_counters) > 10_000:
                cutoff = bucket - 2
                stale = [k for k, (b, _) in self._memory_counters.items() if b < cutoff]
                for k in stale:
                    del self._memory_counters[k]
            counter_bucket, counter_value = self._memory_counters.get(namespaced, (bucket, 0))
            if counter_bucket != bucket:
                counter_value = 0
            counter_value += 1
            self._memory_counters[namespaced] = (bucket, counter_value)
            if counter_value > limit:
                raise RateLimitError()


rate_limiter = RateLimiter()


def ping_redis() -> bool:
    return rate_limiter.ping()
