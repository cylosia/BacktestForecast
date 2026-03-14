"""Tests for Python entry point compilation and config validation."""
from __future__ import annotations

import py_compile
import pathlib

import pytest


_ENTRYPOINTS = [
    "apps/worker/app/tasks.py",
    "apps/worker/app/celery_app.py",
    "apps/api/app/main.py",
]


@pytest.mark.parametrize("path", _ENTRYPOINTS)
def test_entrypoints_compile(path: str) -> None:
    resolved = pathlib.Path(path).resolve()
    py_compile.compile(str(resolved), doraise=True)


def test_sse_rate_limit_validated_to_minimum_one() -> None:
    from backtestforecast.config import Settings

    s = Settings(
        sse_rate_limit=0,
        clerk_jwt_key="test-key",
        _env_file=None,
    )
    assert s.sse_rate_limit >= 1


def test_sse_redis_max_connections_validated_to_minimum_one() -> None:
    from backtestforecast.config import Settings

    s = Settings(
        sse_redis_max_connections=0,
        clerk_jwt_key="test-key",
        _env_file=None,
    )
    assert s.sse_redis_max_connections >= 1


def test_sse_redis_socket_timeout_validated_to_minimum() -> None:
    from backtestforecast.config import Settings

    s = Settings(
        sse_redis_socket_timeout=0.0,
        clerk_jwt_key="test-key",
        _env_file=None,
    )
    assert s.sse_redis_socket_timeout >= 0.1


def test_sse_redis_connect_timeout_validated_to_minimum() -> None:
    from backtestforecast.config import Settings

    s = Settings(
        sse_redis_connect_timeout=0.0,
        clerk_jwt_key="test-key",
        _env_file=None,
    )
    assert s.sse_redis_connect_timeout >= 0.1


def test_to_decimal_raises_on_nan() -> None:
    """Item 89: to_decimal must raise ValueError for float('nan')."""
    from decimal import Decimal

    from backtestforecast.services.backtests import to_decimal

    import pytest

    with pytest.raises(ValueError, match="Non-finite"):
        to_decimal(float("nan"))


def test_to_decimal_raises_on_inf() -> None:
    """to_decimal must also raise ValueError for float('inf')."""
    from backtestforecast.services.backtests import to_decimal

    import pytest

    with pytest.raises(ValueError, match="Non-finite"):
        to_decimal(float("inf"))


def test_to_decimal_raises_on_decimal_nan() -> None:
    """to_decimal must raise ValueError for Decimal('NaN')."""
    from decimal import Decimal

    from backtestforecast.services.backtests import to_decimal

    import pytest

    with pytest.raises(ValueError, match="Non-finite"):
        to_decimal(Decimal("NaN"))


def test_db_pool_max_overflow_validated_to_minimum_one() -> None:
    from backtestforecast.config import Settings

    s = Settings(
        db_pool_max_overflow=0,
        clerk_jwt_key="test-key",
        _env_file=None,
    )
    assert s.db_pool_max_overflow >= 1
