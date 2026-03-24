"""Tests for CircuitBreaker trip and recovery behaviour."""
from __future__ import annotations

import time

import pytest

from backtestforecast.resilience.circuit_breaker import CircuitBreaker, CircuitState


@pytest.fixture()
def cb() -> CircuitBreaker:
    return CircuitBreaker(
        name="test",
        failure_threshold=3,
        recovery_timeout=0.2,
        half_open_max_calls=1,
        probe_timeout=5.0,
    )


class TestClosedState:
    def test_allows_calls_when_closed(self, cb: CircuitBreaker) -> None:
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True

    def test_stays_closed_below_threshold(self, cb: CircuitBreaker) -> None:
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True

    def test_success_resets_failure_count(self, cb: CircuitBreaker) -> None:
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED


class TestTripping:
    def test_trips_after_reaching_threshold(self, cb: CircuitBreaker) -> None:
        for _ in range(cb.failure_threshold):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_open_rejects_requests(self, cb: CircuitBreaker) -> None:
        for _ in range(cb.failure_threshold):
            cb.record_failure()
        assert cb.allow_request() is False

    def test_non_transient_failures_do_not_trip(self, cb: CircuitBreaker) -> None:
        for _ in range(cb.failure_threshold + 5):
            cb.record_failure(is_transient=False)
        assert cb.state == CircuitState.CLOSED


class TestHalfOpen:
    def test_transitions_to_half_open_after_recovery_timeout(self, cb: CircuitBreaker) -> None:
        for _ in range(cb.failure_threshold):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN
        time.sleep(cb.recovery_timeout + 0.05)
        assert cb.state == CircuitState.HALF_OPEN

    def test_half_open_allows_one_probe(self, cb: CircuitBreaker) -> None:
        for _ in range(cb.failure_threshold):
            cb.record_failure()
        time.sleep(cb.recovery_timeout + 0.05)
        assert cb.allow_request() is True
        assert cb.allow_request() is False


class TestRecovery:
    def test_success_in_half_open_closes_circuit(self, cb: CircuitBreaker) -> None:
        for _ in range(cb.failure_threshold):
            cb.record_failure()
        time.sleep(cb.recovery_timeout + 0.05)
        assert cb.allow_request() is True
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True

    def test_failure_in_half_open_reopens_circuit(self, cb: CircuitBreaker) -> None:
        for _ in range(cb.failure_threshold):
            cb.record_failure()
        time.sleep(cb.recovery_timeout + 0.05)
        assert cb.allow_request() is True
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.allow_request() is False


class TestReset:
    def test_reset_returns_to_closed(self, cb: CircuitBreaker) -> None:
        for _ in range(cb.failure_threshold):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN
        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True
