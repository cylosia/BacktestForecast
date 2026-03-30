from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from sqlalchemy import update
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.config import get_settings
from backtestforecast.models import MultiStepRun, MultiSymbolRun
from tests.integration.test_api_critical_flows import _set_user_plan


def _multi_symbol_payload() -> dict:
    return {
        "name": "Lifecycle multi-symbol run",
        "symbols": [
            {
                "symbol": "SPY",
                "risk_per_trade_pct": "2.5",
                "max_open_positions": 1,
            },
            {
                "symbol": "QQQ",
                "risk_per_trade_pct": "2.5",
                "max_open_positions": 1,
            },
        ],
        "strategy_groups": [
            {
                "name": "paired momentum",
                "synchronous_entry": True,
                "legs": [
                    {
                        "symbol": "SPY",
                        "strategy_type": "long_call",
                        "target_dte": 30,
                        "dte_tolerance_days": 5,
                        "max_holding_days": 10,
                        "quantity_mode": "fixed_contracts",
                        "fixed_contracts": 1,
                    },
                    {
                        "symbol": "QQQ",
                        "strategy_type": "long_call",
                        "target_dte": 30,
                        "dte_tolerance_days": 5,
                        "max_holding_days": 10,
                        "quantity_mode": "fixed_contracts",
                        "fixed_contracts": 1,
                    },
                ],
            }
        ],
        "entry_rules": [
            {
                "left_symbol": "SPY",
                "left_indicator": "close",
                "operator": "gt",
                "right_symbol": "QQQ",
                "right_indicator": "close",
            }
        ],
        "exit_rules": [],
        "start_date": "2024-01-02",
        "end_date": "2024-03-29",
        "account_size": "10000",
        "capital_allocation_mode": "equal_weight",
        "commission_per_contract": "0.65",
        "slippage_pct": "0.10",
    }


def _multi_step_payload() -> dict:
    return {
        "name": "Lifecycle multi-step run",
        "symbol": "SPY",
        "workflow_type": "calendar_roll_premium",
        "start_date": "2024-01-02",
        "end_date": "2024-03-29",
        "account_size": "10000",
        "risk_per_trade_pct": "2.5",
        "commission_per_contract": "0.65",
        "slippage_pct": "0.10",
        "initial_entry_rules": [
            {"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}
        ],
        "steps": [
            {
                "step_number": 1,
                "name": "open starter",
                "action": "open_position",
                "trigger": {
                    "mode": "date_offset",
                    "days_after_prior_step": 0,
                    "rules": [],
                },
                "contract_selection": {
                    "strategy_type": "long_call",
                    "target_dte": 30,
                    "dte_tolerance_days": 5,
                    "max_holding_days": 10,
                },
                "failure_policy": "liquidate",
            },
            {
                "step_number": 2,
                "name": "close starter",
                "action": "close_position",
                "trigger": {
                    "mode": "date_offset",
                    "days_after_prior_step": 5,
                    "rules": [],
                },
                "contract_selection": {
                    "strategy_type": "long_call",
                    "target_dte": 30,
                    "dte_tolerance_days": 5,
                    "max_holding_days": 10,
                },
                "failure_policy": "liquidate",
            },
        ],
    }


@pytest.fixture()
def immediate_multi_workflow_completion(_fake_celery, session_factory: sessionmaker[Session]) -> None:
    def _finish_multi_symbol(name: str, kwargs: dict[str, str]) -> None:
        assert name == "multi_symbol_backtests.run"
        with session_factory() as session:
            run_id = UUID(kwargs["run_id"])
            now = datetime.now(UTC)
            session.execute(
                update(MultiSymbolRun)
                .where(MultiSymbolRun.id == run_id)
                .values(
                    status="succeeded",
                    started_at=now,
                    completed_at=now,
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
                    ending_equity=Decimal("10000"),
                    expectancy=Decimal("0"),
                )
            )
            session.commit()

    def _finish_multi_step(name: str, kwargs: dict[str, str]) -> None:
        assert name == "multi_step_backtests.run"
        with session_factory() as session:
            run_id = UUID(kwargs["run_id"])
            now = datetime.now(UTC)
            session.execute(
                update(MultiStepRun)
                .where(MultiStepRun.id == run_id)
                .values(
                    status="succeeded",
                    started_at=now,
                    completed_at=now,
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
                    ending_equity=Decimal("10000"),
                    expectancy=Decimal("0"),
                )
            )
            session.commit()

    _fake_celery.register("multi_symbol_backtests.run", _finish_multi_symbol)
    _fake_celery.register("multi_step_backtests.run", _finish_multi_step)


@pytest.fixture()
def enabled_multi_workflow_flags(client):
    settings = get_settings().model_copy(update={
        "feature_backtests_enabled": True,
        "feature_multi_symbol_backtests_enabled": True,
        "feature_multi_step_backtests_enabled": True,
    })
    client.app.dependency_overrides[get_settings] = lambda: settings
    try:
        yield
    finally:
        client.app.dependency_overrides.pop(get_settings, None)


def test_multi_symbol_endpoint_lifecycle(
    client,
    auth_headers,
    db_session,
    immediate_multi_workflow_completion,
    enabled_multi_workflow_flags,
):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    create = client.post("/v1/multi-symbol-backtests", json=_multi_symbol_payload(), headers=auth_headers)
    assert create.status_code == 202
    created = create.json()
    run_id = created["id"]
    assert created["status"] in {"queued", "running", "succeeded"}
    assert created["name"] == "Lifecycle multi-symbol run"
    assert created["summary"]["trade_count"] == 0

    detail = client.get(f"/v1/multi-symbol-backtests/{run_id}", headers=auth_headers)
    assert detail.status_code == 200
    detail_body = detail.json()
    assert detail_body["id"] == run_id
    assert detail_body["status"] == "succeeded"
    assert [item["symbol"] for item in detail_body["symbols"]] == ["SPY", "QQQ"]

    status = client.get(f"/v1/multi-symbol-backtests/{run_id}/status", headers=auth_headers)
    assert status.status_code == 200
    assert status.json()["status"] == "succeeded"

    listing = client.get("/v1/multi-symbol-backtests", headers=auth_headers)
    assert listing.status_code == 200
    items = listing.json()["items"]
    assert any(item["id"] == run_id for item in items)


def test_multi_step_endpoint_lifecycle(
    client,
    auth_headers,
    db_session,
    immediate_multi_workflow_completion,
    enabled_multi_workflow_flags,
):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    create = client.post("/v1/multi-step-backtests", json=_multi_step_payload(), headers=auth_headers)
    assert create.status_code == 202
    created = create.json()
    run_id = created["id"]
    assert created["status"] in {"queued", "running", "succeeded"}
    assert created["symbol"] == "SPY"
    assert created["workflow_type"] == "calendar_roll_premium"

    detail = client.get(f"/v1/multi-step-backtests/{run_id}", headers=auth_headers)
    assert detail.status_code == 200
    detail_body = detail.json()
    assert detail_body["id"] == run_id
    assert detail_body["status"] == "succeeded"
    assert detail_body["symbol"] == "SPY"
    assert detail_body["summary"]["trade_count"] == 0

    status = client.get(f"/v1/multi-step-backtests/{run_id}/status", headers=auth_headers)
    assert status.status_code == 200
    assert status.json()["status"] == "succeeded"

    listing = client.get("/v1/multi-step-backtests", headers=auth_headers)
    assert listing.status_code == 200
    items = listing.json()["items"]
    assert any(item["id"] == run_id for item in items)


def test_multi_symbol_create_is_feature_locked_when_dedicated_flag_is_disabled(
    client,
    auth_headers,
    db_session,
):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    settings = get_settings().model_copy(update={
        "feature_backtests_enabled": True,
        "feature_multi_symbol_backtests_enabled": False,
    })
    client.app.dependency_overrides[get_settings] = lambda: settings
    try:
        response = client.post("/v1/multi-symbol-backtests", json=_multi_symbol_payload(), headers=auth_headers)
    finally:
        client.app.dependency_overrides.pop(get_settings, None)

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "feature_locked"


def test_multi_step_create_is_feature_locked_when_dedicated_flag_is_disabled(
    client,
    auth_headers,
    db_session,
):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    settings = get_settings().model_copy(update={
        "feature_backtests_enabled": True,
        "feature_multi_step_backtests_enabled": False,
    })
    client.app.dependency_overrides[get_settings] = lambda: settings
    try:
        response = client.post("/v1/multi-step-backtests", json=_multi_step_payload(), headers=auth_headers)
    finally:
        client.app.dependency_overrides.pop(get_settings, None)

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "feature_locked"


def test_multi_symbol_cancel_then_delete_path(
    client,
    auth_headers,
    db_session,
    _fake_celery,
    enabled_multi_workflow_flags,
):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    create = client.post("/v1/multi-symbol-backtests", json=_multi_symbol_payload(), headers=auth_headers)
    assert create.status_code == 202
    run_id = create.json()["id"]
    assert create.json()["status"] == "queued"

    delete_queued = client.delete(f"/v1/multi-symbol-backtests/{run_id}", headers=auth_headers)
    assert delete_queued.status_code == 409

    cancel = client.post(f"/v1/multi-symbol-backtests/{run_id}/cancel", headers=auth_headers)
    assert cancel.status_code == 200
    cancel_body = cancel.json()
    assert cancel_body["status"] == "cancelled"
    assert cancel_body["error_code"] == "cancelled_by_user"

    delete_cancelled = client.delete(f"/v1/multi-symbol-backtests/{run_id}", headers=auth_headers)
    assert delete_cancelled.status_code == 204

    detail = client.get(f"/v1/multi-symbol-backtests/{run_id}", headers=auth_headers)
    assert detail.status_code == 404


def test_multi_step_cancel_then_delete_path(
    client,
    auth_headers,
    db_session,
    _fake_celery,
    enabled_multi_workflow_flags,
):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")

    create = client.post("/v1/multi-step-backtests", json=_multi_step_payload(), headers=auth_headers)
    assert create.status_code == 202
    run_id = create.json()["id"]
    assert create.json()["status"] == "queued"

    delete_queued = client.delete(f"/v1/multi-step-backtests/{run_id}", headers=auth_headers)
    assert delete_queued.status_code == 409

    cancel = client.post(f"/v1/multi-step-backtests/{run_id}/cancel", headers=auth_headers)
    assert cancel.status_code == 200
    cancel_body = cancel.json()
    assert cancel_body["status"] == "cancelled"
    assert cancel_body["error_code"] == "cancelled_by_user"

    delete_cancelled = client.delete(f"/v1/multi-step-backtests/{run_id}", headers=auth_headers)
    assert delete_cancelled.status_code == 204

    detail = client.get(f"/v1/multi-step-backtests/{run_id}", headers=auth_headers)
    assert detail.status_code == 404
