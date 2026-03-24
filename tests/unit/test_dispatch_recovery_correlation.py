from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.models import BacktestRun, OutboxMessage, User
from backtestforecast.services.dispatch_recovery import DISPATCH_SLA, get_dispatch_target, get_queue_diagnostics
from tests.conftest import strip_partial_indexes_for_sqlite as _strip_partial_indexes_for_sqlite


@pytest.fixture()
def db_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    _strip_partial_indexes_for_sqlite(engine)
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


def test_queue_diagnostics_ignore_outbox_rows_for_other_task_types(db_session) -> None:
    user = User(clerk_user_id="dispatch-correlation-user", email="dispatch-correlation@example.com")
    db_session.add(user)
    db_session.flush()

    created_at = datetime.now(UTC) - (DISPATCH_SLA + timedelta(minutes=1))
    run = BacktestRun(
        user_id=user.id,
        symbol="AAPL",
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
    run.created_at = created_at
    db_session.add(run)
    db_session.flush()

    db_session.add(
        OutboxMessage(
            task_name="exports.generate",
            task_kwargs_json={"export_job_id": str(run.id)},
            queue="exports",
            status="pending",
            correlation_id=run.id,
        )
    )
    db_session.commit()

    diagnostics = get_queue_diagnostics(
        db_session,
        now=created_at + DISPATCH_SLA + timedelta(minutes=2),
        targets=(get_dispatch_target("BacktestRun"),),
    )

    assert diagnostics["stale_without_outbox_total"] == 1
    assert diagnostics["models"]["BacktestRun"]["stale_without_outbox"] == 1
