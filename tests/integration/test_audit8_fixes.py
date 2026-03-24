from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

import pytest
from pydantic import ValidationError as PydanticValidationError
from sqlalchemy.exc import SQLAlchemyError

from backtestforecast.errors import ConfigurationError
from backtestforecast.schemas.backtests import CreateBacktestRunRequest
from backtestforecast.services.backtests import to_decimal
from backtestforecast.services.templates import _resolve_template_limit


def test_health_ready_returns_200_when_db_healthy(client):
    with patch("apps.api.app.routers.health.ping_database"):
        resp = client.get("/health/ready")
    assert resp.status_code == 200
    assert resp.json()["status"] in ("ok", "degraded")


def test_health_ready_returns_503_when_db_down(client):
    with patch("apps.api.app.routers.health.ping_database", side_effect=SQLAlchemyError("connection refused")):
        resp = client.get("/health/ready")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"


def test_health_ready_returns_503_when_redis_down_and_fail_closed(client, monkeypatch):
    monkeypatch.setattr("apps.api.app.routers.health.ping_redis", lambda: False)
    monkeypatch.setattr("apps.api.app.routers.health._ping_broker_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health.ping_database", lambda: None)
    monkeypatch.setattr("apps.api.app.routers.health.get_settings", lambda: type(
        "S",
        (),
        {
            "metrics_token": None,
            "app_env": "test",
            "rate_limit_fail_closed": True,
            "rate_limit_degraded_memory_fallback": False,
            "massive_api_key": None,
            "sentry_dsn": None,
        },
    )())

    resp = client.get("/health/ready")

    assert resp.status_code == 503
    assert resp.json()["status"] == "unavailable"


def test_health_ready_reports_degraded_memory_fallback_mode_with_details(client, monkeypatch):
    settings = type(
        "S",
        (),
        {
            "metrics_token": "secret",
            "app_env": "test",
            "rate_limit_fail_closed": False,
            "rate_limit_degraded_memory_fallback": True,
            "massive_api_key": None,
            "sentry_dsn": None,
        },
    )()
    monkeypatch.setattr("apps.api.app.routers.health.ping_redis", lambda: False)
    monkeypatch.setattr("apps.api.app.routers.health._ping_broker_redis", lambda: True)
    monkeypatch.setattr("apps.api.app.routers.health.ping_database", lambda: None)
    monkeypatch.setattr("apps.api.app.routers.health.get_settings", lambda: settings)

    resp = client.get("/health/ready", headers={"x-metrics-token": "secret"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["redis"] == "down"
    assert body["rate_limit_mode"] == "degraded_memory_fallback"


def test_quota_counts_queued_and_running_runs(client, auth_headers, db_session, _fake_celery):
    from backtestforecast.models import BacktestRun, User

    client.get("/v1/me", headers=auth_headers)
    user = db_session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()

    today = datetime.now(UTC).date()
    start = today - timedelta(days=90)
    for i, status in enumerate(["queued", "running", "succeeded", "queued", "running"]):
        run = BacktestRun(
            user_id=user.id,
            status=status,
            symbol=f"Q{i:02d}X",
            strategy_type="long_call",
            date_from=start,
            date_to=today - timedelta(days=1),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("5"),
            commission_per_contract=Decimal("1"),
            input_snapshot_json={},
            warnings_json=[],
        )
        db_session.add(run)
    db_session.commit()

    resp = client.post(
        "/v1/backtests",
        json={
            "symbol": "OVER",
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
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "quota_exceeded"


def test_to_decimal_returns_none_for_nan():
    assert to_decimal(float("nan")) is None


def test_to_decimal_returns_none_for_nan_with_allow_infinite():
    assert to_decimal(float("nan"), allow_infinite=True) is None


def test_to_decimal_returns_none_for_infinity_when_allowed():
    assert to_decimal(float("inf"), allow_infinite=True) is None
    assert to_decimal(float("-inf"), allow_infinite=True) is None
    assert to_decimal(Decimal("Infinity"), allow_infinite=True) is None


def test_to_decimal_raises_for_infinity_when_not_allowed():
    with pytest.raises(ValueError, match="Non-finite"):
        to_decimal(float("inf"))


def test_to_decimal_returns_none_for_decimal_nan():
    assert to_decimal(Decimal("NaN")) is None


def test_end_date_rejects_future_date():
    tomorrow = date.today() + timedelta(days=1)
    with pytest.raises(PydanticValidationError, match="end_date cannot be in the future"):
        CreateBacktestRunRequest(
            symbol="AAPL",
            strategy_type="long_call",
            start_date=date(2024, 1, 2),
            end_date=tomorrow,
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("5"),
            commission_per_contract=Decimal("1"),
            entry_rules=[{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}],
        )


def test_dte_tolerance_must_be_less_than_target_dte():
    yesterday = date.today() - timedelta(days=1)
    with pytest.raises(PydanticValidationError, match=r"dte_tolerance_days .* must be less than target_dte"):
        CreateBacktestRunRequest(
            symbol="AAPL",
            strategy_type="long_call",
            start_date=date(2024, 1, 2),
            end_date=yesterday,
            target_dte=30,
            dte_tolerance_days=30,
            max_holding_days=10,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("5"),
            commission_per_contract=Decimal("1"),
            entry_rules=[{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}],
        )


def test_template_limit_raises_for_unknown_tier():
    with patch("backtestforecast.services.templates.normalize_plan_tier", return_value="unknown"):
        with pytest.raises(ConfigurationError, match="Unknown plan tier for template limit"):
            _resolve_template_limit("enterprise", "active")
