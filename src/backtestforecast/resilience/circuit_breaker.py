"""Circuit breaker for external API calls with optional Redis-backed
cluster-wide state sharing.  When *redis_client* is provided the failure
count and open/closed state are stored in Redis so all workers share the
same circuit.  Without Redis the breaker falls back to per-process state.
"""
from __future__ import annotations

import time
import threading
from enum import Enum
from typing import Any

import structlog

from backtestforecast.observability.metrics import CIRCUIT_BREAKER_STATE, CIRCUIT_BREAKER_TRIPS_TOTAL

logger = structlog.get_logger("resilience.circuit_breaker")


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 1,
        probe_timeout: float = 30.0,
        redis_client: Any | None = None,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.probe_timeout = probe_timeout
        self._redis = redis_client
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: float | None = None
        self._half_open_calls = 0
        self._probe_in_flight = False
        self._probe_started_at: float | None = None
        self._lock = threading.Lock()
        self._redis_key = f"bff:circuit:{name}"
        self._redis_check_interval = 2.0  # seconds between Redis cluster checks
        self._last_redis_check: float = 0.0
        self._last_redis_result: bool | None = None

    @property
    def state(self) -> CircuitState:
        with self._lock:
            if self._state == CircuitState.OPEN and self._last_failure_time is not None:
                if time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    self._probe_in_flight = False
            return self._state

    @property
    def is_open(self) -> bool:
        """True when the circuit is not fully closed (OPEN or HALF_OPEN).

        Does NOT trigger the OPEN -> HALF_OPEN recovery transition.
        Use ``allow_request()`` for gating decisions.
        """
        with self._lock:
            return self._state != CircuitState.CLOSED

    def _update_state_gauge(self) -> None:
        state_val = {CircuitState.CLOSED: 0, CircuitState.HALF_OPEN: 1, CircuitState.OPEN: 2}
        CIRCUIT_BREAKER_STATE.labels(service=self.name).set(state_val.get(self._state, 0))

    def record_success(self) -> None:
        with self._lock:
            self._failure_count = 0
            self._state = CircuitState.CLOSED
            self._half_open_calls = 0
            self._probe_in_flight = False
            self._probe_started_at = None
            self._redis_reset()
            self._update_state_gauge()

    def _redis_record_failure(self) -> int | None:
        """Increment cluster-wide failure counter, return new count."""
        if self._redis is None:
            return None
        try:
            pipe = self._redis.pipeline(transaction=True)
            pipe.incr(self._redis_key)
            pipe.expire(self._redis_key, int(self.recovery_timeout) + 10)
            result = pipe.execute()
            return int(result[0])
        except Exception:
            logger.debug("circuit_breaker.redis_failure_record_failed", name=self.name)
            return None

    def _redis_is_open(self) -> bool | None:
        """Check cluster-wide failure count; returns None on Redis error."""
        if self._redis is None:
            return None
        try:
            val = self._redis.get(self._redis_key)
            if val is None:
                return False
            return int(val) >= self.failure_threshold
        except Exception:
            return None

    def _redis_reset(self) -> None:
        if self._redis is None:
            return
        try:
            self._redis.delete(self._redis_key)
        except Exception:
            pass

    def record_failure(self, *, is_transient: bool = True) -> None:
        with self._lock:
            if not is_transient:
                logger.debug("circuit_breaker.non_transient_failure", name=self.name)
                return
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            redis_count = self._redis_record_failure()
            effective_count = max(self._failure_count, redis_count or 0)
            was_closed = self._state == CircuitState.CLOSED
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._probe_in_flight = False
                self._probe_started_at = None
            elif effective_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._probe_in_flight = False
                self._probe_started_at = None
            if was_closed and self._state == CircuitState.OPEN:
                CIRCUIT_BREAKER_TRIPS_TOTAL.labels(service=self.name).inc()
            self._update_state_gauge()

    def allow_request(self) -> bool:
        with self._lock:
            if self._state == CircuitState.CLOSED:
                now = time.monotonic()
                if now - self._last_redis_check >= self._redis_check_interval:
                    self._last_redis_result = self._redis_is_open()
                    self._last_redis_check = now
                cluster_open = self._last_redis_result
                if cluster_open:
                    self._state = CircuitState.OPEN
                    self._last_failure_time = time.monotonic()
                    self._update_state_gauge()
            if self._state == CircuitState.HALF_OPEN and self._probe_in_flight:
                if (
                    self._probe_started_at is not None
                    and time.monotonic() - self._probe_started_at > self.probe_timeout
                ):
                    self._probe_in_flight = False
                    self._probe_started_at = None
                    logger.warning("circuit_breaker.probe_timeout", name=self.name)
            if self._state == CircuitState.OPEN and self._last_failure_time is not None:
                if time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    self._probe_in_flight = False
                    self._probe_started_at = None
            if self._state == CircuitState.CLOSED:
                return True
            if self._state == CircuitState.HALF_OPEN:
                if self._probe_in_flight:
                    return False
                if self._half_open_calls < self.half_open_max_calls:
                    self._half_open_calls += 1
                    self._probe_in_flight = True
                    self._probe_started_at = time.monotonic()
                    return True
                return False
            return False

    def reset(self) -> None:
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._last_failure_time = None
            self._half_open_calls = 0
            self._probe_in_flight = False
            self._probe_started_at = None
            self._redis_reset()
            self._update_state_gauge()

    async def allow_request_async(self) -> bool:
        """Non-blocking version of allow_request for async callers."""
        import asyncio
        return await asyncio.to_thread(self.allow_request)

    async def record_success_async(self) -> None:
        import asyncio
        await asyncio.to_thread(self.record_success)

    async def record_failure_async(self, *, is_transient: bool = True) -> None:
        import asyncio
        await asyncio.to_thread(self.record_failure, is_transient=is_transient)
