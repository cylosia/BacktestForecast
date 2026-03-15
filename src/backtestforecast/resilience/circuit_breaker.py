"""Simple circuit breaker for external API calls."""
from __future__ import annotations

import asyncio
import time
import threading
from enum import Enum

from backtestforecast.observability.metrics import CIRCUIT_BREAKER_STATE, CIRCUIT_BREAKER_TRIPS_TOTAL


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
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: float | None = None
        self._half_open_calls = 0
        self._probe_in_flight = False
        self._lock = threading.Lock()

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
            self._update_state_gauge()

    def record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            was_closed = self._state == CircuitState.CLOSED
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._probe_in_flight = False
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._probe_in_flight = False
            if was_closed and self._state == CircuitState.OPEN:
                CIRCUIT_BREAKER_TRIPS_TOTAL.labels(service=self.name).inc()
            self._update_state_gauge()

    def allow_request(self) -> bool:
        with self._lock:
            if self._state == CircuitState.OPEN and self._last_failure_time is not None:
                if time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    self._probe_in_flight = False
            if self._state == CircuitState.CLOSED:
                return True
            if self._state == CircuitState.HALF_OPEN:
                if self._probe_in_flight:
                    return False
                if self._half_open_calls < self.half_open_max_calls:
                    self._half_open_calls += 1
                    self._probe_in_flight = True
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
            self._update_state_gauge()

    async def allow_request_async(self) -> bool:
        """Non-blocking version of allow_request for async callers."""
        return await asyncio.to_thread(self.allow_request)

    async def record_success_async(self) -> None:
        await asyncio.to_thread(self.record_success)

    async def record_failure_async(self) -> None:
        await asyncio.to_thread(self.record_failure)
