"""Tests for Python entry point compilation and config validation."""
from __future__ import annotations

import pathlib
import py_compile

import pytest
from pydantic import ValidationError

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

    with pytest.raises(ValidationError, match="sse_rate_limit"):
        Settings(
            sse_rate_limit=0,
            clerk_jwt_key="test-key",
            _env_file=None,
        )


def test_sse_redis_max_connections_validated_to_minimum_one() -> None:
    from backtestforecast.config import Settings

    with pytest.raises(ValidationError, match="sse_redis_max_connections"):
        Settings(
            sse_redis_max_connections=0,
            clerk_jwt_key="test-key",
            _env_file=None,
        )


def test_sse_redis_socket_timeout_validated_to_minimum() -> None:
    from backtestforecast.config import Settings

    with pytest.raises(ValidationError, match="sse_redis_socket_timeout"):
        Settings(
            sse_redis_socket_timeout=0.0,
            clerk_jwt_key="test-key",
            _env_file=None,
        )


def test_sse_redis_connect_timeout_validated_to_minimum() -> None:
    from backtestforecast.config import Settings

    with pytest.raises(ValidationError, match="sse_redis_connect_timeout"):
        Settings(
            sse_redis_connect_timeout=0.0,
            clerk_jwt_key="test-key",
            _env_file=None,
        )


def test_to_decimal_rejects_nan() -> None:
    """to_decimal returns None for float('nan') to avoid serialization crashes."""
    from backtestforecast.services.backtests import to_decimal

    assert to_decimal(float("nan")) is None


def test_to_decimal_rejects_inf() -> None:
    """to_decimal must reject float('inf') with ValueError."""
    from backtestforecast.services.backtests import to_decimal

    with pytest.raises(ValueError, match="Non-finite"):
        to_decimal(float("inf"))


def test_to_decimal_rejects_decimal_nan() -> None:
    """to_decimal returns None for Decimal('NaN') to avoid serialization crashes."""
    from backtestforecast.services.backtests import to_decimal

    assert to_decimal(__import__("decimal").Decimal("NaN")) is None


def test_db_pool_max_overflow_allows_zero() -> None:
    from backtestforecast.config import Settings

    settings = Settings(
        db_pool_max_overflow=0,
        clerk_jwt_key="test-key",
        _env_file=None,
    )

    assert settings.db_pool_max_overflow == 0


def test_db_pool_max_overflow_rejects_negative_values() -> None:
    from backtestforecast.config import Settings

    with pytest.raises(ValidationError, match="db_pool_max_overflow"):
        Settings(
            db_pool_max_overflow=-1,
            clerk_jwt_key="test-key",
            _env_file=None,
        )


def test_daily_picks_endpoint_has_offset_param():
    """Verify get_latest_daily_picks endpoint accepts offset query param."""
    import inspect

    from apps.api.app.routers.daily_picks import get_latest_daily_picks
    sig = inspect.signature(get_latest_daily_picks)
    assert "offset" in sig.parameters, "daily_picks endpoint must accept offset parameter"


def test_wheel_resolve_exit_receives_profit_target():
    """Verify wheel backtest loop passes profit_target_pct to _resolve_exit."""
    import inspect

    from backtestforecast.backtests.strategies.wheel import WheelStrategyBacktestEngine
    source = inspect.getsource(WheelStrategyBacktestEngine.run)
    assert "profit_target_pct" in source, "Wheel.run must pass profit_target_pct to _resolve_exit"
    assert "capital_at_risk" in source, "Wheel.run must pass capital_at_risk to _resolve_exit"
