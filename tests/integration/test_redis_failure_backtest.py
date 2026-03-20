"""Integration tests: backtest creation when Redis is unavailable.

When rate_limit_fail_closed is True (default), the rate limiter should
reject requests with ServiceUnavailableError when Redis connection fails.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from redis.exceptions import RedisError

from backtestforecast.errors import ServiceUnavailableError
from backtestforecast.security.rate_limits import RateLimiter, get_rate_limiter


def test_rate_limiter_raises_service_unavailable_when_redis_fails_and_fail_closed():
    """RateLimiter raises ServiceUnavailableError when Redis fails and fail_closed=True."""
    settings = MagicMock()
    settings.rate_limit_prefix = "test"
    settings.rate_limit_fail_closed = True
    settings.rate_limit_degraded_memory_fallback = False
    settings.rate_limit_memory_max_keys = 10_000
    settings.redis_cache_url = "redis://localhost:6379/0"

    limiter = RateLimiter(settings=settings)
    mock_redis = MagicMock()
    mock_redis.evalsha.side_effect = RedisError("connection refused")
    mock_redis.script_load.side_effect = RedisError("connection refused")
    limiter._get_redis = lambda: mock_redis  # type: ignore[assignment]
    limiter._redis = mock_redis

    with pytest.raises(ServiceUnavailableError) as exc_info:
        limiter.check(
            bucket="backtests:create",
            actor_key="user-123",
            limit=10,
            window_seconds=60,
        )
    assert exc_info.value.status_code == 503


def test_backtest_create_returns_503_when_redis_unavailable(
    client,
    auth_headers,
    monkeypatch,
):
    """When Redis is unavailable and fail_closed, POST /v1/backtests returns 503."""
    from datetime import UTC, datetime, timedelta

    today = datetime.now(UTC).date()
    start = today - timedelta(days=90)

    # Mock Redis to fail - rate limiter should raise ServiceUnavailableError
    rl = get_rate_limiter()
    mock_redis = MagicMock()
    mock_redis.evalsha.side_effect = RedisError("connection refused")
    mock_redis.script_load.side_effect = RedisError("connection refused")
    monkeypatch.setattr(rl, "_get_redis", lambda: mock_redis)
    monkeypatch.setattr(rl, "_redis", mock_redis)

    resp = client.post(
        "/v1/backtests",
        json={
            "symbol": "AAPL",
            "strategy_type": "long_call",
            "start_date": str(start),
            "end_date": str(today - timedelta(days=1)),
            "target_dte": 30,
            "dte_tolerance_days": 5,
            "max_holding_days": 10,
            "account_size": "10000",
            "risk_per_trade_pct": "5",
            "commission_per_contract": "1",
            "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}],
        },
        headers=auth_headers,
    )

    assert resp.status_code == 503
    body = resp.json()
    assert body.get("error", {}).get("code") == "service_unavailable"
