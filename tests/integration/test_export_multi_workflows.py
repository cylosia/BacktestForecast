from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from backtestforecast.models import (
    MultiStepEquityPoint,
    MultiStepRun,
    MultiStepTrade,
    MultiSymbolEquityPoint,
    MultiSymbolRun,
    MultiSymbolTrade,
    MultiSymbolTradeGroup,
    User,
)
from tests.integration.test_api_critical_flows import _set_user_plan


def _get_user(session) -> User:
    return session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()


def _create_multi_symbol_run(session) -> MultiSymbolRun:
    user = _get_user(session)
    run = MultiSymbolRun(
        user_id=user.id,
        status="succeeded",
        name="Exportable multi-symbol run",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 2, 29),
        account_size=Decimal("10000"),
        capital_allocation_mode="equal_weight",
        commission_per_contract=Decimal("0.65"),
        slippage_pct=Decimal("0.10"),
        input_snapshot_json={
            "symbols": [{"symbol": "UVXY"}, {"symbol": "VIX"}, {"symbol": "SPY"}],
            "strategy_groups": [{"name": "uvxy_signal_group"}],
        },
        trade_count=1,
        win_rate=Decimal("100"),
        total_roi_pct=Decimal("4.2"),
        average_win_amount=Decimal("420"),
        average_loss_amount=Decimal("0"),
        average_holding_period_days=Decimal("3"),
        average_dte_at_open=Decimal("21"),
        max_drawdown_pct=Decimal("1.5"),
        total_commissions=Decimal("2.60"),
        total_net_pnl=Decimal("417.40"),
        starting_equity=Decimal("10000"),
        ending_equity=Decimal("10417.40"),
        expectancy=Decimal("417.40"),
        created_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )
    session.add(run)
    session.flush()

    trade_group = MultiSymbolTradeGroup(
        run_id=run.id,
        entry_date=date(2024, 1, 10),
        exit_date=date(2024, 1, 15),
        status="closed",
    )
    session.add(trade_group)
    session.flush()

    session.add(
        MultiSymbolTrade(
            run_id=run.id,
            trade_group_id=trade_group.id,
            symbol="UVXY",
            option_ticker="UVXY240202C00050000",
            strategy_type="long_call",
            entry_date=date(2024, 1, 10),
            exit_date=date(2024, 1, 15),
            expiration_date=date(2024, 2, 2),
            quantity=1,
            dte_at_open=23,
            holding_period_days=5,
            entry_underlying_close=Decimal("48.00"),
            exit_underlying_close=Decimal("52.00"),
            entry_mid=Decimal("1.20"),
            exit_mid=Decimal("5.40"),
            gross_pnl=Decimal("420.00"),
            net_pnl=Decimal("417.40"),
            total_commissions=Decimal("2.60"),
            entry_reason="signal_sync",
            exit_reason="target_hit",
            detail_json={"paired_symbols": ["VIX", "SPY"]},
        )
    )
    session.add(
        MultiSymbolEquityPoint(
            run_id=run.id,
            trade_date=date(2024, 1, 15),
            equity=Decimal("10417.40"),
            cash=Decimal("10417.40"),
            position_value=Decimal("0"),
            drawdown_pct=Decimal("0"),
        )
    )
    session.commit()
    session.refresh(run)
    return run


def _create_multi_step_run(session) -> MultiStepRun:
    user = _get_user(session)
    run = MultiStepRun(
        user_id=user.id,
        status="succeeded",
        name="Exportable multi-step run",
        symbol="SPY",
        workflow_type="calendar_roll_premium",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 2, 29),
        account_size=Decimal("12000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("0.65"),
        slippage_pct=Decimal("0.10"),
        input_snapshot_json={},
        trade_count=1,
        win_rate=Decimal("100"),
        total_roi_pct=Decimal("2.8"),
        average_win_amount=Decimal("336"),
        average_loss_amount=Decimal("0"),
        average_holding_period_days=Decimal("7"),
        average_dte_at_open=Decimal("21"),
        max_drawdown_pct=Decimal("0.8"),
        total_commissions=Decimal("2.60"),
        total_net_pnl=Decimal("333.40"),
        starting_equity=Decimal("12000"),
        ending_equity=Decimal("12333.40"),
        expectancy=Decimal("333.40"),
        created_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )
    session.add(run)
    session.flush()

    session.add(
        MultiStepTrade(
            run_id=run.id,
            step_number=2,
            option_ticker="SPY240202C00490000",
            strategy_type="calendar_spread",
            entry_date=date(2024, 1, 12),
            exit_date=date(2024, 1, 19),
            expiration_date=date(2024, 2, 2),
            quantity=1,
            dte_at_open=21,
            holding_period_days=7,
            entry_underlying_close=Decimal("480.00"),
            exit_underlying_close=Decimal("486.00"),
            entry_mid=Decimal("2.40"),
            exit_mid=Decimal("5.76"),
            gross_pnl=Decimal("336.00"),
            net_pnl=Decimal("333.40"),
            total_commissions=Decimal("2.60"),
            entry_reason="step_execute",
            exit_reason="step_liquidate",
            detail_json={"step_number": 2},
        )
    )
    session.add(
        MultiStepEquityPoint(
            run_id=run.id,
            trade_date=date(2024, 1, 19),
            equity=Decimal("12333.40"),
            cash=Decimal("12333.40"),
            position_value=Decimal("0"),
            drawdown_pct=Decimal("0"),
        )
    )
    session.commit()
    session.refresh(run)
    return run


def test_export_csv_for_multi_symbol_run(client, auth_headers, db_session, immediate_export_execution):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    run = _create_multi_symbol_run(db_session)

    export = client.post("/v1/exports", json={"run_id": str(run.id), "format": "csv"}, headers=auth_headers)
    assert export.status_code == 202
    body = export.json()
    assert body["status"] == "succeeded"
    assert body["run_id"] == str(run.id)

    download = client.get(f"/v1/exports/{body['id']}", headers=auth_headers)
    assert download.status_code == 200
    text = download.content.decode("utf-8")
    assert "UVXY+VIX+SPY" in text
    assert "uvxy_signal_group" in text
    assert "UVXY240202C00050000" in text


def test_export_csv_for_multi_step_run(client, auth_headers, db_session, immediate_export_execution):
    client.get("/v1/me", headers=auth_headers)
    _set_user_plan(db_session, tier="pro", subscription_status="active")
    run = _create_multi_step_run(db_session)

    export = client.post("/v1/exports", json={"run_id": str(run.id), "format": "csv"}, headers=auth_headers)
    assert export.status_code == 202
    body = export.json()
    assert body["status"] == "succeeded"
    assert body["run_id"] == str(run.id)

    download = client.get(f"/v1/exports/{body['id']}", headers=auth_headers)
    assert download.status_code == 200
    text = download.content.decode("utf-8")
    assert "SPY" in text
    assert "calendar_roll_premium" in text
    assert "SPY240202C00490000" in text
