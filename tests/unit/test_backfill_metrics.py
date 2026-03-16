"""Test: backfill_metrics script.

Verifies that the backfill function correctly identifies runs missing
metrics and fills them in from trades + equity curve data.
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.models import BacktestEquityPoint, BacktestRun, BacktestTrade, User


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    yield session
    session.close()
    engine.dispose()


def _seed_user(session) -> User:
    user = User(
        id=uuid4(),
        clerk_user_id="user_backfill",
        email="backfill@test.com",
        plan_tier="pro",
        created_at=datetime.now(UTC),
    )
    session.add(user)
    session.commit()
    return user


def _seed_run_with_trades(session, user_id) -> BacktestRun:
    """Create a succeeded run with profit_factor=None and one trade/equity point."""
    run = BacktestRun(
        id=uuid4(),
        user_id=user_id,
        symbol="AAPL",
        strategy_type="long_call",
        status="succeeded",
        date_from=date(2023, 1, 1),
        date_to=date(2023, 6, 30),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=30,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        input_snapshot_json={"symbol": "AAPL"},
        starting_equity=Decimal("10000"),
        ending_equity=Decimal("10500"),
        total_roi_pct=Decimal("5.0"),
        max_drawdown_pct=Decimal("2.0"),
        win_rate=Decimal("60.0"),
        trade_count=1,
        total_net_pnl=Decimal("500"),
        total_commissions=Decimal("5"),
        average_win_amount=Decimal("500"),
        average_loss_amount=Decimal("0"),
        average_holding_period_days=Decimal("14"),
        average_dte_at_open=Decimal("20"),
        expectancy=Decimal("500"),
        profit_factor=None,
        created_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )
    session.add(run)
    session.flush()

    winning_trade = BacktestTrade(
        id=uuid4(),
        run_id=run.id,
        option_ticker="AAPL230120C00150000",
        strategy_type="long_call",
        underlying_symbol="AAPL",
        entry_date=date(2023, 1, 1),
        exit_date=date(2023, 1, 15),
        expiration_date=date(2023, 1, 20),
        quantity=1,
        dte_at_open=20,
        holding_period_days=14,
        entry_underlying_close=Decimal("150.00"),
        exit_underlying_close=Decimal("155.00"),
        entry_mid=Decimal("5.00"),
        exit_mid=Decimal("7.50"),
        gross_pnl=Decimal("750.00"),
        net_pnl=Decimal("745.00"),
        total_commissions=Decimal("5.00"),
        entry_reason="signal",
        exit_reason="target",
    )
    session.add(winning_trade)

    losing_trade = BacktestTrade(
        id=uuid4(),
        run_id=run.id,
        option_ticker="AAPL230215C00160000",
        strategy_type="long_call",
        underlying_symbol="AAPL",
        entry_date=date(2023, 2, 1),
        exit_date=date(2023, 2, 15),
        expiration_date=date(2023, 2, 17),
        quantity=1,
        dte_at_open=17,
        holding_period_days=14,
        entry_underlying_close=Decimal("160.00"),
        exit_underlying_close=Decimal("155.00"),
        entry_mid=Decimal("6.00"),
        exit_mid=Decimal("3.50"),
        gross_pnl=Decimal("-250.00"),
        net_pnl=Decimal("-255.00"),
        total_commissions=Decimal("5.00"),
        entry_reason="signal",
        exit_reason="max_holding_days",
    )
    session.add(losing_trade)

    point = BacktestEquityPoint(
        id=uuid4(),
        run_id=run.id,
        trade_date=date(2023, 1, 1),
        equity=Decimal("10000.00"),
        cash=Decimal("5000.00"),
        position_value=Decimal("5000.00"),
        drawdown_pct=Decimal("0.00"),
    )
    session.add(point)
    session.commit()
    return run


def test_backfill_updates_missing_metrics(db_session):
    """Runs with profit_factor=None should be updated by the backfill script."""
    user = _seed_user(db_session)
    run = _seed_run_with_trades(db_session, user.id)

    assert run.profit_factor is None
    run_id = run.id

    with patch(
        "backtestforecast.management.backfill_metrics.SessionLocal",
        return_value=db_session,
    ):
        from backtestforecast.management.backfill_metrics import backfill

        count = backfill()

    assert count >= 1
    db_session.expire_all()
    from sqlalchemy import select
    refreshed = db_session.scalar(select(BacktestRun).where(BacktestRun.id == run_id))
    assert refreshed is not None
    assert refreshed.profit_factor is not None


def test_backfill_skips_already_filled(db_session):
    """Runs that already have profit_factor should be skipped."""
    user = _seed_user(db_session)
    run = _seed_run_with_trades(db_session, user.id)
    run.profit_factor = Decimal("1.5")
    db_session.commit()

    with patch(
        "backtestforecast.management.backfill_metrics.SessionLocal",
        return_value=db_session,
    ):
        from backtestforecast.management.backfill_metrics import backfill

        count = backfill()

    assert count == 0
