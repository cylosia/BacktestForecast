from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from backtestforecast.models import BacktestRun, ExportJob, User

pytestmark = pytest.mark.integration


def test_backtest_remediation_actions_surface_cancel_delete_state(client, auth_headers, immediate_backtest_execution):
    payload = {
        "symbol": "AAPL",
        "strategy_type": "long_call",
        "start_date": "2024-01-02",
        "end_date": "2024-03-29",
        "target_dte": 30,
        "dte_tolerance_days": 5,
        "max_holding_days": 10,
        "account_size": "10000",
        "risk_per_trade_pct": "5",
        "commission_per_contract": "1",
        "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "35", "period": 14}],
    }
    created = client.post("/v1/backtests", json=payload, headers=auth_headers)
    assert created.status_code == 202
    run_id = created.json()["id"]

    response = client.get(f"/v1/backtests/{run_id}/remediation-actions", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    actions = {item["action"]: item for item in body["actions"]}

    assert body["resource_type"] == "backtest"
    assert actions["cancel"]["allowed"] is False
    assert actions["delete"]["allowed"] is True


def test_export_remediation_actions_surface_retry_for_failed_export(client, auth_headers, db_session):
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
        input_snapshot_json={
            "risk_free_rate": 0.0125,
            "resolved_risk_free_rate_source": "massive_treasury",
        },
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

    export_job = ExportJob(
        user_id=user.id,
        backtest_run_id=run.id,
        export_format="csv",
        status="failed",
        file_name="aapl.csv",
        mime_type="text/csv",
        error_code="export_generation_failed",
        error_message="Export generation failed. Please try again.",
    )
    db_session.add(export_job)
    db_session.commit()

    response = client.get(f"/v1/exports/{export_job.id}/remediation-actions", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    actions = {item["action"]: item for item in body["actions"]}

    assert body["resource_type"] == "export"
    assert actions["retry"]["allowed"] is True
    assert actions["delete"]["allowed"] is True
