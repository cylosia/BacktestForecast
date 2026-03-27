"""Tests for _fallback_persist_status terminal-state guard."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.models import BacktestRun, User

pytestmark = pytest.mark.postgres

@pytest.fixture()
def db_session_factory(postgres_session_factory: sessionmaker[Session]):
    return postgres_session_factory


@pytest.fixture()
def db_session(postgres_db_session: Session) -> Session:
    return postgres_db_session


def _create_user(session: Session) -> User:
    user = User(clerk_user_id=f"test_{uuid4().hex[:8]}", email="fallback@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_backtest_run(session: Session, user: User, status: str) -> BacktestRun:
    run = BacktestRun(
        user_id=user.id,
        symbol="AAPL",
        strategy_type="long_call",
        status=status,
        date_from=date(2024, 1, 1),
        date_to=date(2024, 3, 31),
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


def test_fallback_persist_status_skips_terminal(db_session, db_session_factory):
    """A run in terminal state ('succeeded') must not be overwritten to 'running'."""
    from backtestforecast.events import _fallback_persist_status

    user = _create_user(db_session)
    run = _create_backtest_run(db_session, user, status="succeeded")

    with patch("backtestforecast.db.session.create_worker_session", side_effect=db_session_factory):
        _fallback_persist_status("backtest", run.id, "failed")

    db_session.expire_all()
    refreshed = db_session.get(BacktestRun, run.id)
    assert refreshed.status == "succeeded"


def test_fallback_persist_status_updates_non_terminal(db_session, db_session_factory):
    """A run in non-terminal state ('running') should be updated to 'failed'."""
    from backtestforecast.events import _fallback_persist_status

    user = _create_user(db_session)
    run = _create_backtest_run(db_session, user, status="running")

    with patch("backtestforecast.db.session.create_worker_session", side_effect=db_session_factory):
        _fallback_persist_status("backtest", run.id, "failed")

    db_session.expire_all()
    refreshed = db_session.get(BacktestRun, run.id)
    assert refreshed.status == "failed"
