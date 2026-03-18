"""Test concurrent task execution for the same job.

Verifies that _validate_task_ownership correctly serialises competing
Celery deliveries so only one worker processes a given job.

Requires Redis for Celery app initialisation — marked as integration.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

pytestmark = pytest.mark.integration

from backtestforecast.db.base import Base
from backtestforecast.models import BacktestRun, User


@pytest.fixture()
def db_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    try:
        yield engine
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture()
def db_session(db_engine) -> Session:
    factory = sessionmaker(bind=db_engine, autoflush=False, expire_on_commit=False)
    session = factory()
    try:
        yield session
    finally:
        session.close()


def _create_user(session: Session) -> User:
    user = User(clerk_user_id="concurrent_test_user", email="concurrent@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def test_concurrent_validate_task_ownership(db_session):
    """Two calls to _validate_task_ownership with the same job but different
    task IDs: only the first should succeed."""
    import apps.worker.app.tasks as tasks_module

    user = _create_user(db_session)
    run = BacktestRun(
        user_id=user.id,
        symbol="SPY",
        strategy_type="long_call",
        status="queued",
        celery_task_id=None,
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
    db_session.add(run)
    db_session.commit()
    db_session.refresh(run)
    run_id = run.id

    first = tasks_module._validate_task_ownership(db_session, BacktestRun, run_id, "task-1")
    second = tasks_module._validate_task_ownership(db_session, BacktestRun, run_id, "task-2")

    assert first is True, "First caller should claim ownership"
    assert second is False, "Second caller should be rejected"
