"""Fix 61: CAS guard prevents resurrecting a reaped backtest run.

When the reaper sets a run's status to "failed" between the queued→running
transition and the success CAS, execute_run_by_id must honour the reaper's
decision and NOT overwrite the status to "succeeded".

Since we cannot easily intercept between execution and CAS in a unit test,
we test the simpler (and equally important) case: a run that has already been
reaped (status="failed") should cause execute_run_by_id to return early
without executing.
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.models import BacktestRun, User


from tests.conftest import strip_partial_indexes_for_sqlite as _strip_partial_indexes_for_sqlite


@pytest.fixture()
def db_session():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    _strip_partial_indexes_for_sqlite(engine)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


def _create_user(session: Session) -> User:
    user = User(clerk_user_id="cas_guard_user", email="cas@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_run(session: Session, user: User, *, status: str) -> BacktestRun:
    run = BacktestRun(
        user_id=user.id,
        status=status,
        symbol="SPY",
        strategy_type="long_call",
        date_from=date(2024, 1, 1),
        date_to=date(2024, 6, 30),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        input_snapshot_json={},
    )
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def test_execute_run_by_id_returns_early_for_failed_status(db_session):
    """A run already marked 'failed' (by the reaper) must not be executed."""
    from unittest.mock import MagicMock

    from backtestforecast.services.backtests import BacktestService

    user = _create_user(db_session)
    run = _create_run(db_session, user, status="failed")

    mock_exec = MagicMock()
    service = BacktestService(db_session, execution_service=mock_exec)
    result = service.execute_run_by_id(run.id)

    assert result.status == "failed", "Run should stay failed"
    mock_exec.execute_request.assert_not_called()


def test_execute_run_by_id_returns_early_for_cancelled_status(db_session):
    """A run already marked 'cancelled' must not be executed."""
    from unittest.mock import MagicMock

    from backtestforecast.services.backtests import BacktestService

    user = _create_user(db_session)
    run = _create_run(db_session, user, status="cancelled")

    mock_exec = MagicMock()
    service = BacktestService(db_session, execution_service=mock_exec)
    result = service.execute_run_by_id(run.id)

    assert result.status == "cancelled", "Run should stay cancelled"
    mock_exec.execute_request.assert_not_called()


def test_execute_run_by_id_returns_early_for_succeeded_status(db_session):
    """A run already marked 'succeeded' must not be re-executed."""
    from unittest.mock import MagicMock

    from backtestforecast.services.backtests import BacktestService

    user = _create_user(db_session)
    run = _create_run(db_session, user, status="succeeded")

    mock_exec = MagicMock()
    service = BacktestService(db_session, execution_service=mock_exec)
    result = service.execute_run_by_id(run.id)

    assert result.status == "succeeded", "Run should stay succeeded"
    mock_exec.execute_request.assert_not_called()


def test_cas_update_skips_when_status_changed_by_reaper(db_session):
    """The CAS UPDATE ... WHERE status = 'running' must affect zero rows
    when the reaper has already changed the status to 'failed'."""
    from sqlalchemy import update

    user = _create_user(db_session)
    run = _create_run(db_session, user, status="failed")

    result = db_session.execute(
        update(BacktestRun)
        .where(BacktestRun.id == run.id, BacktestRun.status == "running")
        .values(status="succeeded", completed_at=datetime.now(UTC))
    )
    db_session.commit()

    assert result.rowcount == 0, "CAS should not match failed row"

    db_session.expire_all()
    refreshed = db_session.get(BacktestRun, run.id)
    assert refreshed.status == "failed", "Status must remain failed"
