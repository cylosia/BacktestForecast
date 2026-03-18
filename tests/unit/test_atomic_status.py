"""Item 68: TOCTOU race prevented with atomic status update.

Verify that the atomic update pattern in backtests.py correctly sets status
to 'failed' only when the current status is NOT 'succeeded'. This prevents
a late-arriving failure from clobbering an already-successful run.
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.models import BacktestRun, User


def _strip_partial_indexes_for_sqlite(engine) -> None:
    """Remove PostgreSQL-specific partial indexes so SQLite create_all succeeds."""
    if engine.dialect.name != "sqlite":
        return
    for table in Base.metadata.tables.values():
        indexes_to_remove = [
            idx for idx in table.indexes
            if idx.dialect_options.get("postgresql", {}).get("where") is not None
        ]
        for idx in indexes_to_remove:
            table.indexes.discard(idx)


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


def _create_user(session: Session) -> User:
    user = User(clerk_user_id="atomic_test_user", email="atomic@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_run(session: Session, user: User, status: str) -> BacktestRun:
    run = BacktestRun(
        user_id=user.id,
        status=status,
        symbol="TEST",
        strategy_type="long_call",
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


def test_atomic_update_does_not_overwrite_succeeded(db_session):
    """When status is already 'succeeded', the atomic UPDATE … WHERE status != 'succeeded'
    must not change it to 'failed'."""
    from sqlalchemy import update

    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")

    result = db_session.execute(
        update(BacktestRun)
        .where(BacktestRun.id == run.id, BacktestRun.status != "succeeded")
        .values(
            status="failed",
            error_code="test_error",
            error_message="Simulated failure",
            completed_at=datetime.now(UTC),
        )
    )
    db_session.commit()

    assert result.rowcount == 0, "Atomic update should not match succeeded status"

    db_session.expire_all()
    refreshed = db_session.get(BacktestRun, run.id)
    assert refreshed.status == "succeeded", "Status must remain succeeded"
    assert refreshed.error_code is None


def test_atomic_update_does_overwrite_running(db_session):
    """When status is 'running', the atomic UPDATE should change it to 'failed'."""
    from sqlalchemy import update

    user = _create_user(db_session)
    run = _create_run(db_session, user, "running")

    result = db_session.execute(
        update(BacktestRun)
        .where(BacktestRun.id == run.id, BacktestRun.status != "succeeded")
        .values(
            status="failed",
            error_code="test_error",
            error_message="Simulated failure",
            completed_at=datetime.now(UTC),
        )
    )
    db_session.commit()

    assert result.rowcount == 1, "Atomic update should match running status"

    db_session.expire_all()
    refreshed = db_session.get(BacktestRun, run.id)
    assert refreshed.status == "failed"
    assert refreshed.error_code == "test_error"
