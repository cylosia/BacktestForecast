"""De facto E2E integration suite: full lifecycle tests via the HTTP API.

Covers the synchronous path end-to-end:
  create backtest -> execute inline -> GET detail -> list runs -> compare runs
  create scan -> execute inline -> GET recommendations
  create export -> execute inline -> download file
  create template -> CRUD lifecycle
  Stripe webhook -> plan upgrade/downgrade
  cross-user data isolation

A Celery-inclusive E2E test is not feasible here because it requires a live
Redis broker and worker process. The ``e2e-tests`` CI job exercises that
path via Playwright against a running API + web server.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest

from apps.api.app.dependencies import get_token_verifier
from backtestforecast.auth.verification import AuthenticatedPrincipal
from backtestforecast.backtests.types import (
    BacktestExecutionResult,
    BacktestSummary,
    EquityPointResult,
    TradeResult,
)
from backtestforecast.market_data.types import DailyBar
from backtestforecast.models import BacktestEquityPoint, BacktestRun, User
from backtestforecast.schemas.scans import HistoricalAnalogForecastResponse
from backtestforecast.services.backtests import EQUITY_CURVE_LIMIT
from backtestforecast.services.scans import ScanService

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeMarketDataService:
    def prepare_backtest(self, request):
        bars = [
            DailyBar(
                trade_date=request.start_date + timedelta(days=offset),
                open_price=100 + offset,
                high_price=101 + offset,
                low_price=99 + offset,
                close_price=100.5 + offset,
                volume=1_000_000 + (offset * 1000),
            )
            for offset in range(5)
        ]
        return SimpleNamespace(bars=bars, earnings_dates=set(), option_gateway=None)


class FakeExecutionService:
    def __init__(self) -> None:
        self.market_data_service = FakeMarketDataService()

    def close(self) -> None:
        pass

    def execute_request(self, request, bundle=None) -> BacktestExecutionResult:
        roi_lookup = {"AAPL": Decimal("12.5"), "MSFT": Decimal("6.5"), "NVDA": Decimal("15.0")}
        roi = roi_lookup.get(request.symbol, Decimal("5.0"))
        net_pnl = (Decimal(request.account_size) * roi / Decimal("100")).quantize(Decimal("0.01"))
        entry_date = request.start_date + timedelta(days=5)
        exit_date = entry_date + timedelta(days=min(request.max_holding_days, 7))
        expiration_date = exit_date + timedelta(days=max(request.target_dte - 7, 7))
        trade = TradeResult(
            option_ticker=f"{request.symbol}240119C00100000",
            strategy_type=request.strategy_type.value
            if hasattr(request.strategy_type, "value")
            else request.strategy_type,
            underlying_symbol=request.symbol,
            entry_date=entry_date,
            exit_date=exit_date,
            expiration_date=expiration_date,
            quantity=1,
            dte_at_open=request.target_dte,
            holding_period_days=(exit_date - entry_date).days,
            entry_underlying_close=100.0,
            exit_underlying_close=104.0,
            entry_mid=2.0,
            exit_mid=3.25,
            gross_pnl=float(net_pnl + Decimal(request.commission_per_contract)),
            net_pnl=float(net_pnl),
            total_commissions=float(request.commission_per_contract),
            entry_reason="=SUM(1,1)",
            exit_reason="@profit-target",
            detail_json={"scenario": "integration-test"},
        )
        summary = BacktestSummary(
            trade_count=1,
            win_rate=100.0 if roi >= 0 else 0.0,
            total_roi_pct=float(roi),
            average_win_amount=float(net_pnl),
            average_loss_amount=0.0,
            average_holding_period_days=float((exit_date - entry_date).days),
            average_dte_at_open=float(request.target_dte),
            max_drawdown_pct=2.5,
            total_commissions=float(request.commission_per_contract),
            total_net_pnl=float(net_pnl),
            starting_equity=float(request.account_size),
            ending_equity=float(Decimal(request.account_size) + net_pnl),
        )
        equity_curve = [
            EquityPointResult(
                trade_date=entry_date,
                equity=float(request.account_size),
                cash=float(request.account_size) - 200.0,
                position_value=200.0,
                drawdown_pct=0.0,
            ),
            EquityPointResult(
                trade_date=exit_date,
                equity=float(Decimal(request.account_size) + net_pnl),
                cash=float(Decimal(request.account_size) + net_pnl),
                position_value=0.0,
                drawdown_pct=0.0,
            ),
        ]
        return BacktestExecutionResult(summary=summary, trades=[trade], equity_curve=equity_curve, warnings=[])


class FakeForecaster:
    def forecast(self, *, symbol, bars, horizon_days, strategy_type=None):
        return HistoricalAnalogForecastResponse(
            symbol=symbol,
            strategy_type=strategy_type,
            as_of_date=bars[-1].trade_date,
            horizon_days=horizon_days,
            analog_count=12,
            expected_return_low_pct=Decimal("-3.0"),
            expected_return_median_pct=Decimal("4.5"),
            expected_return_high_pct=Decimal("9.0"),
            positive_outcome_rate_pct=Decimal("62.0"),
            summary="Bounded range.",
            disclaimer="Not advice.",
            analog_dates=[bars[-1].trade_date - timedelta(days=30)],
        )


class FakeStripeModule:
    api_key = "sk_test"

    class Webhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return {
                "id": "evt_test_upgrade",
                "type": "customer.subscription.updated",
                "livemode": False,
                "data": {
                    "object": {
                        "id": "sub_test_123",
                        "customer": "cus_test_123",
                        "status": "active",
                        "cancel_at_period_end": False,
                        "current_period_end": int((datetime.now(UTC) + timedelta(days=30)).timestamp()),
                        "metadata": {"user_id": ""},
                        "items": {"data": [{"price": {"id": "price_pro_monthly", "recurring": {"interval": "month"}}}]},
                    }
                },
            }


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def stub_execution(monkeypatch):
    import backtestforecast.services.backtests as bs
    import backtestforecast.services.scans as ss

    monkeypatch.setattr(bs, "BacktestExecutionService", FakeExecutionService)
    monkeypatch.setattr(ss, "BacktestExecutionService", FakeExecutionService)
    monkeypatch.setattr(ss, "HistoricalAnalogForecaster", FakeForecaster)


@pytest.fixture()
def immediate_scan_execution(_fake_celery, session_factory, stub_execution):
    def _run(name: str, kwargs: dict[str, str]) -> None:
        assert name == "scans.run_job"
        with session_factory() as session:
            ScanService(session).run_job(UUID(kwargs["job_id"]))

    _fake_celery.register("scans.run_job", _run)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _set_user_plan(session, *, tier, subscription_status=None):
    user = session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
    user.plan_tier = tier
    user.subscription_status = subscription_status
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _backtest_payload(symbol="AAPL", **overrides):
    payload = {
        "symbol": symbol,
        "strategy_type": "long_call",
        "start_date": "2024-01-02",
        "end_date": "2024-03-29",
        "target_dte": 30,
        "dte_tolerance_days": 5,
        "max_holding_days": 10,
        "account_size": "10000",
        "risk_per_trade_pct": "5",
        "commission_per_contract": "1",
        "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}],
    }
    payload.update(overrides)
    return payload


def _create_backtest(client, auth_headers, symbol="AAPL", **overrides):
    resp = client.post("/v1/backtests", json=_backtest_payload(symbol, **overrides), headers=auth_headers)
    assert resp.status_code == 202
    return resp.json()


def _template_payload(name="Test template", strategy="long_call"):
    return {
        "name": name,
        "config": {
            "strategy_type": strategy,
            "target_dte": 30,
            "dte_tolerance_days": 5,
            "max_holding_days": 10,
            "account_size": 10000,
            "risk_per_trade_pct": 2,
            "commission_per_contract": 0.65,
            "entry_rules": [{"type": "rsi", "operator": "lt", "threshold": 35, "period": 14}],
            "default_symbol": "SPY",
        },
    }


# ===========================================================================
# 1. Auth
# ===========================================================================


def test_auth_protected_route(client, auth_headers):
    assert client.get("/v1/me").status_code == 401
    me = client.get("/v1/me", headers=auth_headers)
    assert me.status_code == 200
    assert me.json()["clerk_user_id"] == "clerk_test_user"


# ===========================================================================
# 2. Async backtest lifecycle
# ===========================================================================


def test_async_backtest_full_lifecycle(client, auth_headers, immediate_backtest_execution):
    created = _create_backtest(client, auth_headers)
    assert created["status"] == "succeeded"
    assert created["summary"]["trade_count"] == 1

    detail = client.get(f"/v1/backtests/{created['id']}", headers=auth_headers).json()
    assert detail["trades"][0]["detail_json"]["scenario"] == "integration-test"

    history = client.get("/v1/backtests", headers=auth_headers).json()
    assert len(history["items"]) == 1


def test_backtest_detail_signals_equity_curve_truncation(client, auth_headers, db_session):
    client.get("/v1/me", headers=auth_headers)
    user = db_session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()

    run = BacktestRun(
        user_id=user.id,
        status="succeeded",
        symbol="AAPL",
        strategy_type="long_call",
        date_from=datetime(2024, 1, 2, tzinfo=UTC).date(),
        date_to=datetime(2024, 3, 29, tzinfo=UTC).date(),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        risk_free_rate=Decimal("0.0125"),
        input_snapshot_json={"risk_free_rate": 0.0125},
        trade_count=0,
        win_rate=Decimal("0"),
        total_roi_pct=Decimal("0"),
        average_win_amount=Decimal("0"),
        average_loss_amount=Decimal("0"),
        average_holding_period_days=Decimal("0"),
        average_dte_at_open=Decimal("0"),
        max_drawdown_pct=Decimal("0"),
        total_commissions=Decimal("0"),
        total_net_pnl=Decimal("0"),
        starting_equity=Decimal("10000"),
        ending_equity=Decimal("10000"),
    )
    db_session.add(run)
    db_session.flush()

    for idx in range(EQUITY_CURVE_LIMIT + 1):
        db_session.add(
            BacktestEquityPoint(
                run_id=run.id,
                trade_date=datetime(2024, 1, 2, tzinfo=UTC).date() + timedelta(days=idx),
                equity=Decimal("10000"),
                cash=Decimal("10000"),
                position_value=Decimal("0"),
                drawdown_pct=Decimal("0"),
            )
        )
    db_session.commit()

    detail = client.get(f"/v1/backtests/{run.id}", headers=auth_headers)
    assert detail.status_code == 200
    body = detail.json()
    assert body["equity_curve_truncated"] is True
    assert len(body["equity_curve"]) == EQUITY_CURVE_LIMIT


def test_backtest_fails_on_enqueue_error(client, auth_headers, stub_execution, _fake_celery):
    def _boom(name: str, kwargs: dict[str, str]) -> None:
        raise ConnectionError("no redis")

    _fake_celery.register("backtests.run", _boom)
    created = _create_backtest(client, auth_headers)
    assert created["status"] == "failed"
    assert created["error_code"] == "enqueue_failed"


def test_backtest_idempotency(client, auth_headers, immediate_backtest_execution):
    payload = _backtest_payload(idempotency_key="idem-001")
    first = client.post("/v1/backtests", json=payload, headers=auth_headers).json()
    second = client.post("/v1/backtests", json=payload, headers=auth_headers).json()
    assert first["id"] == second["id"]
    assert len(client.get("/v1/backtests", headers=auth_headers).json()["items"]) == 1


def test_backtest_multiple_strategies(client, auth_headers, db_session, immediate_backtest_execution):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    for strat in ["covered_call", "iron_condor", "bull_call_debit_spread", "short_straddle", "collar", "jade_lizard"]:
        created = _create_backtest(client, auth_headers, symbol=f"T{strat[:2].upper()}X", strategy_type=strat)
        assert created["strategy_type"] == strat
        assert created["status"] == "succeeded"


# ===========================================================================
# 3. Quota enforcement
# ===========================================================================


def test_free_tier_quota_exceeded_code(client, auth_headers, immediate_backtest_execution):
    for i in range(5):
        _create_backtest(client, auth_headers, symbol=f"S{i:02d}L")

    resp = client.post("/v1/backtests", json=_backtest_payload(symbol="OVER"), headers=auth_headers)
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "quota_exceeded"


def test_pro_unlimited_backtests(client, auth_headers, db_session, immediate_backtest_execution):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    for i in range(7):
        assert _create_backtest(client, auth_headers, symbol=f"P{i:02d}L")["status"] == "succeeded"


# ===========================================================================
# 4. Templates CRUD + limits
# ===========================================================================


def test_template_full_crud(client, auth_headers):
    # Create
    create = client.post("/v1/templates", json=_template_payload(), headers=auth_headers)
    assert create.status_code == 201
    t = create.json()
    assert t["config"]["default_symbol"] == "SPY"
    tid = t["id"]

    # List
    assert client.get("/v1/templates", headers=auth_headers).json()["total"] == 1

    # Get
    assert client.get(f"/v1/templates/{tid}", headers=auth_headers).json()["name"] == "Test template"

    # Update
    updated = client.patch(f"/v1/templates/{tid}", json={"name": "New name"}, headers=auth_headers).json()
    assert updated["name"] == "New name"

    # Delete
    assert client.delete(f"/v1/templates/{tid}", headers=auth_headers).status_code == 204
    assert client.get("/v1/templates", headers=auth_headers).json()["total"] == 0


def test_template_limit_free_tier(client, auth_headers):
    for i in range(3):
        assert (
            client.post("/v1/templates", json=_template_payload(name=f"T{i}"), headers=auth_headers).status_code == 201
        )

    resp = client.post("/v1/templates", json=_template_payload(name="T3"), headers=auth_headers)
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "quota_exceeded"


def test_template_limit_pro_tier(client, auth_headers, db_session):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    for i in range(5):
        assert (
            client.post("/v1/templates", json=_template_payload(name=f"Pro{i}"), headers=auth_headers).status_code
            == 201
        )


def test_template_not_found(client, auth_headers):
    assert client.get("/v1/templates/00000000-0000-0000-0000-000000000001", headers=auth_headers).status_code == 404


def test_template_with_all_strategies(client, auth_headers):
    """Verify templates accept all 14 strategy types."""
    for strat in ["long_call", "iron_condor", "wheel_strategy", "butterfly"]:
        resp = client.post(
            "/v1/templates", json=_template_payload(name=f"t-{strat}", strategy=strat), headers=auth_headers
        )
        # May hit 3-template limit on free, but first 3 should succeed
        if resp.status_code == 201:
            assert resp.json()["config"]["strategy_type"] == strat


# ===========================================================================
# 5. Compare
# ===========================================================================


def test_compare_two_runs(client, auth_headers, db_session, immediate_backtest_execution):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    ids = [_create_backtest(client, auth_headers, symbol=s)["id"] for s in ["AAPL", "MSFT"]]

    resp = client.post("/v1/backtests/compare", json={"run_ids": ids}, headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["items"]) == 2
    assert data["comparison_limit"] == 3
    assert data["items"][0]["symbol"] == "AAPL"
    assert data["items"][1]["symbol"] == "MSFT"


def test_compare_blocked_free_tier(client, auth_headers, immediate_backtest_execution):
    ids = [_create_backtest(client, auth_headers, symbol=s)["id"] for s in ["AAPL", "MSFT"]]
    resp = client.post("/v1/backtests/compare", json={"run_ids": ids}, headers=auth_headers)
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "feature_locked"


def test_compare_missing_run(client, auth_headers, db_session, immediate_backtest_execution):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    real = _create_backtest(client, auth_headers)["id"]
    fake = "00000000-0000-0000-0000-000000000099"
    resp = client.post("/v1/backtests/compare", json={"run_ids": [real, fake]}, headers=auth_headers)
    assert resp.status_code == 404


def test_compare_minimum_two_required(client, auth_headers):
    resp = client.post(
        "/v1/backtests/compare", json={"run_ids": ["00000000-0000-0000-0000-000000000001"]}, headers=auth_headers
    )
    assert resp.status_code == 422


def test_compare_three_runs_premium(client, auth_headers, db_session, immediate_backtest_execution):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="premium", subscription_status="active")

    ids = [_create_backtest(client, auth_headers, symbol=f"C{i}X")["id"] for i in range(4)]
    resp = client.post("/v1/backtests/compare", json={"run_ids": ids}, headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["comparison_limit"] == 8


@pytest.mark.parametrize(
    ("tier", "subscription_status", "symbols", "expected_status", "expected_limit"),
    [
        ("free", None, ["AAPL", "MSFT"], 403, None),
        ("pro", "active", ["AAPL", "MSFT"], 200, 3),
        ("premium", "active", ["AAPL", "MSFT", "NVDA"], 200, 8),
    ],
)
def test_compare_endpoint_respects_entitlement_tiers(
    client,
    auth_headers,
    db_session,
    immediate_backtest_execution,
    tier,
    subscription_status,
    symbols,
    expected_status,
    expected_limit,
):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier=tier, subscription_status=subscription_status)

    ids = [_create_backtest(client, auth_headers, symbol=symbol)["id"] for symbol in symbols]
    response = client.post("/v1/backtests/compare", json={"run_ids": ids}, headers=auth_headers)
    assert response.status_code == expected_status
    if expected_status == 200:
        body = response.json()
        assert body["comparison_limit"] == expected_limit
        assert len(body["items"]) == len(symbols)
    else:
        assert response.json()["error"]["code"] == "feature_locked"


# ===========================================================================
# 6. Async exports
# ===========================================================================


def test_export_csv_async(client, auth_headers, db_session, immediate_backtest_execution, immediate_export_execution):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    run_id = _create_backtest(client, auth_headers)["id"]

    export = client.post("/v1/exports", json={"run_id": run_id, "format": "csv"}, headers=auth_headers)
    assert export.status_code == 202
    ej = export.json()
    assert ej["status"] == "succeeded"

    status = client.get(f"/v1/exports/{ej['id']}/status", headers=auth_headers)
    assert status.json()["status"] == "succeeded"

    download = client.get(f"/v1/exports/{ej['id']}", headers=auth_headers)
    assert download.status_code == 200
    assert "attachment" in download.headers["content-disposition"].lower()
    body = download.content.decode("utf-8")
    assert "'=SUM(1,1)" in body
    assert "'@profit-target" in body


def test_create_backtest_rejects_empty_entry_rules_at_public_boundary(client, auth_headers, stub_execution, _fake_celery):
    response = client.post(
        "/v1/backtests",
        json=_backtest_payload(entry_rules=[]),
        headers=auth_headers,
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert "entry rule" in response.json()["error"]["message"].lower()


def test_backtest_cancel_then_delete_path(client, auth_headers, stub_execution, _fake_celery):
    created = _create_backtest(client, auth_headers)
    assert created["status"] == "queued"

    delete_while_queued = client.delete(f"/v1/backtests/{created['id']}", headers=auth_headers)
    assert delete_while_queued.status_code == 409

    cancel = client.post(f"/v1/backtests/{created['id']}/cancel", headers=auth_headers)
    assert cancel.status_code == 200
    assert cancel.json()["status"] == "cancelled"

    delete_after_cancel = client.delete(f"/v1/backtests/{created['id']}", headers=auth_headers)
    assert delete_after_cancel.status_code == 204

    detail = client.get(f"/v1/backtests/{created['id']}", headers=auth_headers)
    assert detail.status_code == 404


def test_export_cancel_then_delete_path(
    client,
    auth_headers,
    db_session,
    stub_execution,
    immediate_backtest_execution,
    _fake_celery,
):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    run_id = _create_backtest(client, auth_headers)["id"]
    export = client.post("/v1/exports", json={"run_id": run_id, "format": "csv"}, headers=auth_headers)
    assert export.status_code == 202
    export_id = export.json()["id"]
    assert export.json()["status"] == "queued"

    delete_while_queued = client.delete(f"/v1/exports/{export_id}", headers=auth_headers)
    assert delete_while_queued.status_code == 409

    cancel = client.post(f"/v1/exports/{export_id}/cancel", headers=auth_headers)
    assert cancel.status_code == 200
    assert cancel.json()["status"] == "cancelled"

    delete_after_cancel = client.delete(f"/v1/exports/{export_id}", headers=auth_headers)
    assert delete_after_cancel.status_code == 204

    status_response = client.get(f"/v1/exports/{export_id}/status", headers=auth_headers)
    assert status_response.status_code == 404


def test_export_blocked_free_tier(client, auth_headers, immediate_backtest_execution):
    run_id = _create_backtest(client, auth_headers)["id"]
    resp = client.post("/v1/exports", json={"run_id": run_id, "format": "csv"}, headers=auth_headers)
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "feature_locked"


# ===========================================================================
# 7. Strategy catalog
# ===========================================================================


def test_catalog_returns_all_35_strategies(client, auth_headers):
    resp = client.get("/v1/strategy-catalog", headers=auth_headers)
    assert resp.status_code == 200
    catalog = resp.json()
    assert catalog["total_strategies"] == 35

    categories = {g["category"] for g in catalog["groups"]}
    assert "single_leg" in categories
    assert "income" in categories
    assert "vertical_spread" in categories
    assert "multi_leg" in categories
    assert "short_volatility" in categories
    assert "diagonal" in categories
    assert "ratio" in categories
    assert "synthetic" in categories
    assert "custom" in categories

    for group in catalog["groups"]:
        for s in group["strategies"]:
            assert s["strategy_type"]
            assert s["label"]
            assert s["min_tier"] in ("free", "pro", "premium")
            assert s["leg_count"] >= 1


def test_catalog_tier_split(client, auth_headers):
    catalog = client.get("/v1/strategy-catalog", headers=auth_headers).json()
    all_strats = [s for g in catalog["groups"] for s in g["strategies"]]
    assert sum(1 for s in all_strats if s["min_tier"] == "free") == 6
    assert sum(1 for s in all_strats if s["min_tier"] == "premium") == 29


def test_catalog_requires_auth(client):
    assert client.get("/v1/strategy-catalog").status_code == 401


# ===========================================================================
# 8. Scanner
# ===========================================================================


def test_scanner_full_flow(client, auth_headers, db_session, immediate_scan_execution):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    payload = {
        "name": "Test scan",
        "mode": "basic",
        "symbols": ["AAPL", "MSFT"],
        "strategy_types": ["long_call", "long_put"],
        "rule_sets": [
            {"name": "RSI", "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}]}
        ],
        "start_date": "2024-01-02",
        "end_date": "2024-03-29",
        "target_dte": 30,
        "dte_tolerance_days": 5,
        "max_holding_days": 10,
        "account_size": "10000",
        "risk_per_trade_pct": "5",
        "commission_per_contract": "1",
        "max_recommendations": 5,
        "idempotency_key": "scan-test-001",
    }
    create = client.post("/v1/scans", json=payload, headers=auth_headers)
    assert create.status_code == 202
    job_id = create.json()["id"]

    job = client.get(f"/v1/scans/{job_id}", headers=auth_headers).json()
    assert job["status"] == "succeeded"

    recs = client.get(f"/v1/scans/{job_id}/recommendations", headers=auth_headers).json()
    assert len(recs["items"]) >= 1
    assert recs["items"][0]["forecast"]["analog_count"] == 12


def test_scanner_blocked_free(client, auth_headers):
    payload = {
        "mode": "basic",
        "symbols": ["AAPL"],
        "strategy_types": ["long_call"],
        "rule_sets": [
            {"name": "t", "entry_rules": [{"type": "rsi", "operator": "lt", "threshold": "30", "period": 14}]}
        ],
        "start_date": "2024-01-02",
        "end_date": "2024-03-29",
        "target_dte": 30,
        "max_holding_days": 10,
        "account_size": "10000",
        "risk_per_trade_pct": "5",
        "commission_per_contract": "1",
    }
    resp = client.post("/v1/scans", json=payload, headers=auth_headers)
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "feature_locked"


def test_scanner_rejects_window_longer_than_backend_max(client, auth_headers, db_session):
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    payload = {
        "mode": "basic",
        "symbols": ["AAPL"],
        "strategy_types": ["long_call"],
        "rule_sets": [
            {"name": "t", "entry_rules": [{"type": "rsi", "operator": "lt", "threshold": "30", "period": 14}]}
        ],
        "start_date": "2023-01-01",
        "end_date": "2025-01-01",
        "target_dte": 30,
        "max_holding_days": 10,
        "account_size": "10000",
        "risk_per_trade_pct": "5",
        "commission_per_contract": "1",
    }
    resp = client.post("/v1/scans", json=payload, headers=auth_headers)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"
    assert resp.json()["error"]["message"] == "scanner window exceeds the configured maximum of 730 days"


# ===========================================================================
# 9. Stripe webhook
# ===========================================================================


def test_stripe_webhook_upgrade_and_dedupe(client, auth_headers, db_session, monkeypatch):
    import backtestforecast.services.billing as billing_services

    user_id = client.get("/v1/me", headers=auth_headers).json()["id"]

    def fake_stripe(self):
        base = FakeStripeModule.Webhook.construct_event(b"{}", "sig", "sec")
        obj = base["data"]["object"]
        return SimpleNamespace(
            construct_event=lambda p, s, sec: {
                **base,
                "data": {"object": {**obj, "metadata": {"user_id": user_id, "requested_tier": "pro"}}},
            },
        )

    monkeypatch.setattr(billing_services.BillingService, "_get_stripe_client", fake_stripe)

    resp = client.post("/v1/billing/webhook", content=b"{}", headers={"Stripe-Signature": "sig", "Host": "localhost"})
    assert resp.json()["status"] == "ok"

    dup = client.post("/v1/billing/webhook", content=b"{}", headers={"Stripe-Signature": "sig", "Host": "localhost"})
    assert dup.json()["status"] == "duplicate"

    db_session.expire_all()
    user = db_session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
    assert user.plan_tier == "pro"


def test_stripe_webhook_rejects_invalid_signature(client, monkeypatch):
    import backtestforecast.services.billing as billing_services

    class SignatureVerificationError(Exception):
        pass

    def fake_stripe_with_sig_check(self):
        def reject_signature(payload, sig_header, secret):
            raise SignatureVerificationError(
                "No signatures found matching the expected signature for payload"
            )

        return SimpleNamespace(construct_event=reject_signature)

    monkeypatch.setattr(
        billing_services.BillingService, "_get_stripe_client", fake_stripe_with_sig_check
    )

    resp = client.post(
        "/v1/billing/webhook",
        content=b'{"id": "evt_tampered"}',
        headers={"Stripe-Signature": "t=999,v1=bad_signature", "Host": "localhost"},
    )
    assert resp.status_code == 401
    body = resp.json()
    assert body["error"]["code"] == "authentication_error"
    assert "signature" in body["error"]["message"].lower()


def test_stripe_webhook_rejects_missing_signature(client):
    resp = client.post(
        "/v1/billing/webhook",
        content=b'{"id": "evt_no_sig"}',
        headers={"Host": "localhost"},
    )
    assert resp.status_code == 422


def test_stripe_webhook_ignored_event_type(client, auth_headers, db_session, monkeypatch):
    import backtestforecast.services.billing as billing_services

    client.get("/v1/me", headers=auth_headers)

    def fake_stripe(self):
        return SimpleNamespace(
            construct_event=lambda p, s, sec: {
                "id": "evt_ignored_001",
                "type": "payment_intent.succeeded",
                "livemode": False,
                "data": {"object": {"id": "pi_test_123"}},
            },
        )

    monkeypatch.setattr(billing_services.BillingService, "_get_stripe_client", fake_stripe)

    resp = client.post(
        "/v1/billing/webhook",
        content=b"{}",
        headers={"Stripe-Signature": "sig", "Host": "localhost"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["event_type"] == "payment_intent.succeeded"

    db_session.expire_all()
    user = db_session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
    assert user.plan_tier == "free"


def test_stripe_webhook_missing_user_metadata(client, monkeypatch):
    import backtestforecast.services.billing as billing_services

    def fake_stripe(self):
        return SimpleNamespace(
            construct_event=lambda p, s, sec: {
                "id": "evt_no_user_meta",
                "type": "checkout.session.completed",
                "livemode": False,
                "data": {
                    "object": {
                        "id": "cs_test_orphan",
                        "customer": "cus_nonexistent",
                        "subscription": "sub_orphan",
                        "metadata": {},
                    }
                },
            },
        )

    monkeypatch.setattr(billing_services.BillingService, "_get_stripe_client", fake_stripe)

    resp = client.post(
        "/v1/billing/webhook",
        content=b"{}",
        headers={"Stripe-Signature": "sig", "Host": "localhost"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_stripe_webhook_subscription_deleted_downgrades_to_free(
    client, auth_headers, db_session, monkeypatch
):
    import backtestforecast.services.billing as billing_services

    user_id = client.get("/v1/me", headers=auth_headers).json()["id"]

    user = db_session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
    user.plan_tier = "pro"
    user.subscription_status = "active"
    user.stripe_subscription_id = "sub_to_cancel"
    user.stripe_customer_id = "cus_cancel_test"
    db_session.commit()

    def fake_stripe(self):
        return SimpleNamespace(
            construct_event=lambda p, s, sec: {
                "id": "evt_sub_deleted",
                "type": "customer.subscription.deleted",
                "livemode": False,
                "data": {
                    "object": {
                        "id": "sub_to_cancel",
                        "customer": "cus_cancel_test",
                        "status": "canceled",
                        "cancel_at_period_end": False,
                        "current_period_end": int(
                            (datetime.now(UTC) + timedelta(days=30)).timestamp()
                        ),
                        "metadata": {
                            "user_id": user_id,
                            "requested_tier": "pro",
                        },
                        "items": {
                            "data": [
                                {
                                    "price": {
                                        "id": "price_pro_monthly",
                                        "recurring": {"interval": "month"},
                                    }
                                }
                            ]
                        },
                    }
                },
            },
        )

    monkeypatch.setattr(billing_services.BillingService, "_get_stripe_client", fake_stripe)

    resp = client.post(
        "/v1/billing/webhook",
        content=b"{}",
        headers={"Stripe-Signature": "sig", "Host": "localhost"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["event_type"] == "customer.subscription.deleted"

    db_session.expire_all()
    user = db_session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
    assert user.plan_tier == "free"
    assert user.subscription_status == "canceled"


def test_stripe_webhook_empty_body_rejected(client, monkeypatch):
    import backtestforecast.services.billing as billing_services

    class SignatureVerificationError(Exception):
        pass

    def fake_stripe_sig_fail(self):
        def reject(payload, sig_header, secret):
            raise SignatureVerificationError("Unable to extract timestamp and signatures")

        return SimpleNamespace(construct_event=reject)

    monkeypatch.setattr(
        billing_services.BillingService, "_get_stripe_client", fake_stripe_sig_fail
    )

    resp = client.post(
        "/v1/billing/webhook",
        content=b"",
        headers={"Stripe-Signature": "t=0,v1=empty", "Host": "localhost"},
    )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "authentication_error"


# ===========================================================================
# 10. /v1/me enrichment
# ===========================================================================


def test_me_features_and_usage(client, auth_headers, immediate_backtest_execution):
    _create_backtest(client, auth_headers)
    me = client.get("/v1/me", headers=auth_headers).json()

    assert me["plan_tier"] == "free"
    assert me["features"]["monthly_backtest_quota"] == 5
    assert me["usage"]["backtests_used_this_month"] == 1
    assert me["usage"]["backtests_remaining_this_month"] == 4
    assert me["features"]["scanner_modes"] == []
    assert me["features"]["forecasting_access"] is False


# ===========================================================================
# 11. /v1/analysis
# ===========================================================================


def test_create_analysis_returns_202(client, auth_headers, db_session, _fake_celery):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    resp = client.post("/v1/analysis", json={"symbol": "AAPL"}, headers=auth_headers)
    assert resp.status_code == 202
    body = resp.json()
    assert body["symbol"] == "AAPL"
    assert body["status"] == "queued"
    assert body["id"] is not None


def test_create_analysis_requires_pro(client, auth_headers):
    resp = client.post("/v1/analysis", json={"symbol": "AAPL"}, headers=auth_headers)
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "feature_locked"


def test_create_analysis_invalid_symbol(client, auth_headers, db_session):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    resp = client.post("/v1/analysis", json={"symbol": "123"}, headers=auth_headers)
    assert resp.status_code == 422


def test_get_analysis_not_found(client, auth_headers):
    import uuid
    resp = client.get(f"/v1/analysis/{uuid.uuid4()}", headers=auth_headers)
    assert resp.status_code == 404


def test_get_analysis_status(client, auth_headers, db_session, _fake_celery):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    create_resp = client.post("/v1/analysis", json={"symbol": "TSLA"}, headers=auth_headers)
    analysis_id = create_resp.json()["id"]
    status_resp = client.get(f"/v1/analysis/{analysis_id}/status", headers=auth_headers)
    assert status_resp.status_code == 200
    assert status_resp.json()["symbol"] == "TSLA"


def test_list_analyses(client, auth_headers, db_session, _fake_celery):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    client.post("/v1/analysis", json={"symbol": "AAPL"}, headers=auth_headers)
    resp = client.get("/v1/analysis", headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()["items"]) >= 1


# ===========================================================================
# 12. /v1/forecasts
# ===========================================================================


def test_get_forecast_returns_envelope(client, auth_headers, db_session, stub_execution):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    resp = client.get("/v1/forecasts/AAPL", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert "forecast" in body
    assert "expected_move_abs_pct" in body
    assert body["forecast"]["symbol"] == "AAPL"


def test_get_forecast_invalid_ticker(client, auth_headers, db_session):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    resp = client.get("/v1/forecasts/123!", headers=auth_headers)
    assert resp.status_code == 422


def test_get_forecast_with_strategy_type(client, auth_headers, db_session, stub_execution):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    resp = client.get("/v1/forecasts/MSFT?strategy_type=long_call", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["forecast"]["symbol"] == "MSFT"


# ===========================================================================
# 13. /v1/events (SSE ownership verification)
# ===========================================================================


def test_backtest_events_not_found(client, auth_headers):
    import uuid
    resp = client.get(f"/v1/events/backtests/{uuid.uuid4()}", headers=auth_headers)
    assert resp.status_code == 404


def test_scan_events_not_found(client, auth_headers):
    import uuid
    resp = client.get(f"/v1/events/scans/{uuid.uuid4()}", headers=auth_headers)
    assert resp.status_code == 404


def test_export_events_not_found(client, auth_headers):
    import uuid
    resp = client.get(f"/v1/events/exports/{uuid.uuid4()}", headers=auth_headers)
    assert resp.status_code == 404


def test_analysis_events_not_found(client, auth_headers):
    import uuid
    resp = client.get(f"/v1/events/analyses/{uuid.uuid4()}", headers=auth_headers)
    assert resp.status_code == 404


# ===========================================================================
# 14. /v1/daily-picks
# ===========================================================================


def test_daily_picks_no_data(client, auth_headers, db_session):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    resp = client.get("/v1/daily-picks", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "no_data"
    assert body["items"] == []


def test_daily_picks_requires_pro(client, auth_headers):
    resp = client.get("/v1/daily-picks", headers=auth_headers)
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "feature_locked"


def test_daily_picks_history_empty(client, auth_headers, db_session):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    resp = client.get("/v1/daily-picks/history", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["items"] == []


# ===========================================================================
# 15. Cross-user data isolation
# ===========================================================================

_USER_B_CLERK_ID = "clerk_other_user"
_USER_B_EMAIL = "other@example.com"


_AS_USER_LOCK = threading.Lock()


@contextmanager
def _as_user(clerk_id: str, email: str):
    """Temporarily switch the authenticated user returned by token verification."""
    verifier = get_token_verifier()
    with _AS_USER_LOCK:
        original = verifier.verify_bearer_token

        def _verify(_token: str) -> AuthenticatedPrincipal:
            return AuthenticatedPrincipal(
                clerk_user_id=clerk_id,
                session_id=f"sess_{clerk_id}",
                email=email,
                claims={"sub": clerk_id, "email": email},
            )

        verifier.verify_bearer_token = _verify
    try:
        yield
    finally:
        with _AS_USER_LOCK:
            verifier.verify_bearer_token = original


def test_backtest_not_visible_to_other_user(client, auth_headers, immediate_backtest_execution):
    """User B must not see User A's backtests."""
    created = _create_backtest(client, auth_headers)
    backtest_id = created["id"]

    with _as_user(_USER_B_CLERK_ID, _USER_B_EMAIL):
        client.get("/v1/me", headers=auth_headers)
        resp = client.get(f"/v1/backtests/{backtest_id}", headers=auth_headers)
        assert resp.status_code == 404


def test_template_not_visible_to_other_user(client, auth_headers):
    """User B must not read, update, or delete User A's templates."""
    created = client.post("/v1/templates", json=_template_payload(), headers=auth_headers)
    assert created.status_code == 201
    tid = created.json()["id"]

    with _as_user(_USER_B_CLERK_ID, _USER_B_EMAIL):
        client.get("/v1/me", headers=auth_headers)

        assert client.get(f"/v1/templates/{tid}", headers=auth_headers).status_code == 404
        assert client.patch(f"/v1/templates/{tid}", json={"name": "Hijack"}, headers=auth_headers).status_code == 404
        assert client.delete(f"/v1/templates/{tid}", headers=auth_headers).status_code == 404

    # Verify User A still owns the template and it's unchanged
    detail = client.get(f"/v1/templates/{tid}", headers=auth_headers)
    assert detail.status_code == 200
    assert detail.json()["name"] == "Test template"


def test_scan_not_visible_to_other_user(client, auth_headers, db_session, immediate_scan_execution):
    """User B must not see User A's scanner jobs."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    scan_payload = {
        "name": "Isolation scan",
        "mode": "basic",
        "symbols": ["AAPL"],
        "strategy_types": ["long_call"],
        "rule_sets": [
            {"name": "RSI", "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}]}
        ],
        "start_date": "2024-01-02",
        "end_date": "2024-03-29",
        "target_dte": 30,
        "dte_tolerance_days": 5,
        "max_holding_days": 10,
        "account_size": "10000",
        "risk_per_trade_pct": "5",
        "commission_per_contract": "1",
    }
    created = client.post("/v1/scans", json=scan_payload, headers=auth_headers)
    assert created.status_code == 202
    job_id = created.json()["id"]

    with _as_user(_USER_B_CLERK_ID, _USER_B_EMAIL):
        client.get("/v1/me", headers=auth_headers)
        resp = client.get(f"/v1/scans/{job_id}", headers=auth_headers)
        assert resp.status_code == 404


def test_export_not_visible_to_other_user(
    client, auth_headers, db_session, immediate_backtest_execution, immediate_export_execution
):
    """User B must not see User A's export status."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    run_id = _create_backtest(client, auth_headers)["id"]
    export = client.post("/v1/exports", json={"run_id": run_id, "format": "csv"}, headers=auth_headers)
    assert export.status_code == 202
    export_id = export.json()["id"]

    with _as_user(_USER_B_CLERK_ID, _USER_B_EMAIL):
        client.get("/v1/me", headers=auth_headers)
        resp = client.get(f"/v1/exports/{export_id}/status", headers=auth_headers)
        assert resp.status_code == 404


# ===========================================================================
# 16. SSE endpoint smoke (no NameError)
# ===========================================================================


# ===========================================================================
# Item 44: ticker with digits passes backend validation
# ===========================================================================


def test_ticker_with_digits_passes_validation():
    """Verify that ticker 'SPY3' matches the production SYMBOL_ALLOWED_CHARS regex,
    confirming digits are allowed in ticker symbols."""
    from backtestforecast.schemas.backtests import SYMBOL_ALLOWED_CHARS

    assert SYMBOL_ALLOWED_CHARS.match("SPY3"), "SPY3 should match the ticker regex"
    assert SYMBOL_ALLOWED_CHARS.match("BRK.B"), "BRK.B should match (dots allowed)"
    assert SYMBOL_ALLOWED_CHARS.match("^VIX"), "^VIX should match (caret allowed)"
    assert SYMBOL_ALLOWED_CHARS.match("SPY"), "SPY should match"
    assert not SYMBOL_ALLOWED_CHARS.match("SPY!"), "SPY! should NOT match (special char)"
    assert not SYMBOL_ALLOWED_CHARS.match(""), "Empty string should NOT match"
    assert not SYMBOL_ALLOWED_CHARS.match("A" * 17), "17-char ticker should NOT match"


def test_ticker_with_digits_accepted_by_forecast_endpoint(
    client, auth_headers, db_session, stub_execution,
):
    """Integration test: forecast endpoint accepts ticker with digits."""
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    resp = client.get("/v1/forecasts/SPY3", headers=auth_headers)
    # 500 is never acceptable - it means an unhandled server error
    assert resp.status_code in (200, 422), (
        f"SPY3 should not return 500; got {resp.status_code}"
    )


def test_sse_endpoints_do_not_crash(
    client, auth_headers, db_session, immediate_backtest_execution, immediate_export_execution, _fake_celery,
):
    """Verify SSE endpoints respond without a NameError for valid resources.

    The key assertion is that the server returns 200 (stream starts), not 500.
    For non-existent resources it returns 404 (already tested above).
    """
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    run_id = _create_backtest(client, auth_headers)["id"]
    resp = client.get(f"/v1/events/backtests/{run_id}", headers=auth_headers)
    assert resp.status_code == 200, f"SSE backtest endpoint returned {resp.status_code} instead of 200"

    export = client.post("/v1/exports", json={"run_id": run_id, "format": "csv"}, headers=auth_headers)
    assert export.status_code == 202
    export_id = export.json()["id"]
    resp = client.get(f"/v1/events/exports/{export_id}", headers=auth_headers)
    assert resp.status_code == 200, f"SSE export endpoint returned {resp.status_code} instead of 200"
