from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.exports.storage import DatabaseStorage
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
from backtestforecast.schemas.exports import CreateExportRequest
from backtestforecast.services.exports import ExportService
from tests.conftest import strip_partial_indexes_for_sqlite as _strip_partial_indexes_for_sqlite


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    _strip_partial_indexes_for_sqlite(engine)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


def _create_user(session) -> User:
    user = User(
        clerk_user_id="export_multi_user",
        email="export-multi@example.com",
        plan_tier="pro",
        subscription_status="active",
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_multi_symbol_run(session, user: User) -> MultiSymbolRun:
    run = MultiSymbolRun(
        user_id=user.id,
        status="succeeded",
        name="Multi-symbol exportable run",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 2, 29),
        account_size=Decimal("10000"),
        capital_allocation_mode="equal_weight",
        commission_per_contract=Decimal("0.65"),
        slippage_pct=Decimal("0.10"),
        input_snapshot_json={
            "symbols": [{"symbol": "TQQQ"}, {"symbol": "SQQQ"}],
            "strategy_groups": [{"name": "paired_short_puts"}],
        },
        trade_count=1,
        win_rate=Decimal("100"),
        total_roi_pct=Decimal("3.5"),
        average_win_amount=Decimal("350"),
        average_loss_amount=Decimal("0"),
        average_holding_period_days=Decimal("5"),
        average_dte_at_open=Decimal("30"),
        max_drawdown_pct=Decimal("1.2"),
        total_commissions=Decimal("2.60"),
        total_net_pnl=Decimal("347.40"),
        starting_equity=Decimal("10000"),
        ending_equity=Decimal("10347.40"),
        expectancy=Decimal("347.40"),
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
            symbol="TQQQ",
            option_ticker="TQQQ240216P00050000",
            strategy_type="cash_secured_put",
            entry_date=date(2024, 1, 10),
            exit_date=date(2024, 1, 15),
            expiration_date=date(2024, 2, 16),
            quantity=1,
            dte_at_open=37,
            holding_period_days=5,
            entry_underlying_close=Decimal("50"),
            exit_underlying_close=Decimal("52"),
            entry_mid=Decimal("1.10"),
            exit_mid=Decimal("4.60"),
            gross_pnl=Decimal("350.00"),
            net_pnl=Decimal("347.40"),
            total_commissions=Decimal("2.60"),
            entry_reason="sync_entry",
            exit_reason="group_exit",
            detail_json={},
        )
    )
    session.add(
        MultiSymbolEquityPoint(
            run_id=run.id,
            trade_date=date(2024, 1, 15),
            equity=Decimal("10347.40"),
            cash=Decimal("10347.40"),
            position_value=Decimal("0"),
            drawdown_pct=Decimal("0"),
        )
    )
    session.commit()
    session.refresh(run)
    return run


def _create_multi_step_run(session, user: User) -> MultiStepRun:
    run = MultiStepRun(
        user_id=user.id,
        status="succeeded",
        name="Multi-step exportable run",
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
            entry_underlying_close=Decimal("480"),
            exit_underlying_close=Decimal("486"),
            entry_mid=Decimal("2.40"),
            exit_mid=Decimal("5.76"),
            gross_pnl=Decimal("336.00"),
            net_pnl=Decimal("333.40"),
            total_commissions=Decimal("2.60"),
            entry_reason="step_execute",
            exit_reason="step_close",
            detail_json={},
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


def test_create_export_supports_multi_symbol_runs(db_session):
    user = _create_user(db_session)
    run = _create_multi_symbol_run(db_session, user)
    service = ExportService(db_session, storage=DatabaseStorage())

    response = service.create_export(
        user,
        CreateExportRequest.model_validate({"run_id": str(run.id), "format": "csv"}),
    )

    assert str(response.run_id) == str(run.id)
    assert response.status == "succeeded"
    content = service.get_db_content_bytes_for_download(user, response.id).decode("utf-8")
    assert "TQQQ+SQQQ" in content
    assert "paired_short_puts" in content
    assert "TQQQ240216P00050000" in content


def test_create_export_supports_multi_step_runs(db_session):
    user = _create_user(db_session)
    run = _create_multi_step_run(db_session, user)
    service = ExportService(db_session, storage=DatabaseStorage())

    response = service.create_export(
        user,
        CreateExportRequest.model_validate({"run_id": str(run.id), "format": "csv"}),
    )

    assert str(response.run_id) == str(run.id)
    assert response.status == "succeeded"
    content = service.get_db_content_bytes_for_download(user, response.id).decode("utf-8")
    assert "SPY" in content
    assert "calendar_roll_premium" in content
    assert "SPY240202C00490000" in content
