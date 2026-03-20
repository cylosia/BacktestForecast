"""Test that circuit breaker Redis I/O runs outside the lock.

The record_success and reset methods must not hold self._lock while
performing Redis operations, to avoid blocking all threads when Redis
is slow or unreachable.
"""
from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

import pytest

from backtestforecast.resilience.circuit_breaker import CircuitBreaker, CircuitState


def test_record_success_does_not_hold_lock_during_redis() -> None:
    """Redis delete in record_success must happen outside the lock."""
    slow_redis = MagicMock()
    lock_held_during_redis = False

    def slow_delete(*args, **kwargs):
        nonlocal lock_held_during_redis
        lock_held_during_redis = cb._lock.locked()
        time.sleep(0.05)

    slow_redis.delete.side_effect = slow_delete

    cb = CircuitBreaker("test", redis_client=slow_redis)
    cb._state = CircuitState.HALF_OPEN
    cb._failure_count = 1
    cb.record_success()

    assert cb._state == CircuitState.CLOSED
    assert not lock_held_during_redis, "Lock was held during Redis delete in record_success"


def test_reset_does_not_hold_lock_during_redis() -> None:
    """Redis delete in reset must happen outside the lock."""
    slow_redis = MagicMock()
    lock_held_during_redis = False

    def slow_delete(*args, **kwargs):
        nonlocal lock_held_during_redis
        lock_held_during_redis = cb._lock.locked()

    slow_redis.delete.side_effect = slow_delete

    cb = CircuitBreaker("test", redis_client=slow_redis)
    cb._state = CircuitState.OPEN
    cb.reset()

    assert cb._state == CircuitState.CLOSED
    assert not lock_held_during_redis, "Lock was held during Redis delete in reset"


def test_record_success_tolerates_redis_failure() -> None:
    """record_success should succeed even if Redis delete fails."""
    failing_redis = MagicMock()
    failing_redis.delete.side_effect = ConnectionError("Redis down")

    cb = CircuitBreaker("test", redis_client=failing_redis)
    cb._state = CircuitState.HALF_OPEN
    cb._failure_count = 3

    cb.record_success()

    assert cb._state == CircuitState.CLOSED
    assert cb._failure_count == 0
