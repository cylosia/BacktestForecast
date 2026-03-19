"""Test the transactional outbox pattern: inline send failure leaves job queued.

When dispatch_celery_task commits the job + OutboxMessage but the inline
Celery send_task fails, the job must remain in "queued" status (not "failed")
so that poll_outbox can recover delivery.
"""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.models import BacktestRun, OutboxMessage, User


@pytest.fixture(autouse=True)
def _mock_celery_module(monkeypatch):
    """Pre-insert a mock celery_app module so dispatch.py's lazy import
    doesn't trigger real settings initialization."""
    import sys
    import types

    mock_celery = MagicMock()
    mock_module = types.ModuleType("apps.worker.app.celery_app")
    mock_module.celery_app = mock_celery
    monkeypatch.setitem(sys.modules, "apps.worker.app.celery_app", mock_module)
    return mock_celery


def _strip_partial_indexes_for_sqlite(engine) -> None:
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
    user = User(clerk_user_id="outbox_test_user", email="outbox@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_queued_run(session: Session, user: User) -> BacktestRun:
    from datetime import date
    run = BacktestRun(
        user_id=user.id,
        status="queued",
        symbol="AAPL",
        strategy_type="covered_call",
        date_from=date(2024, 1, 1),
        date_to=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
    )
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


class TestOutboxDispatchRecovery:
    def test_job_stays_queued_when_inline_send_fails_and_outbox_committed(self, db_session, _mock_celery_module):
        """When the Celery send_task call fails but the OutboxMessage was
        committed, the job must stay in 'queued' (not 'failed') so
        poll_outbox can recover it."""
        import structlog
        from apps.api.app.dispatch import DispatchResult, dispatch_celery_task

        user = _create_user(db_session)
        run = _create_queued_run(db_session, user)

        _mock_celery_module.send_task.side_effect = ConnectionError("broker down")

        result = dispatch_celery_task(
            db=db_session,
            job=run,
            task_name="backtests.run",
            task_kwargs={"run_id": str(run.id)},
            queue="research",
            log_event="backtest",
            logger=structlog.get_logger("test"),
        )

        assert result == DispatchResult.ENQUEUE_FAILED

        db_session.expire_all()
        refreshed = db_session.get(BacktestRun, run.id)
        assert refreshed.status == "queued", (
            f"Job should stay 'queued' for outbox recovery, got '{refreshed.status}'"
        )
        assert refreshed.celery_task_id is not None, (
            "celery_task_id should be set even when send failed"
        )

    def test_outbox_message_stays_pending_for_poller(self, db_session, _mock_celery_module):
        """The OutboxMessage created by dispatch must stay 'pending' so
        poll_outbox picks it up."""
        import structlog
        from apps.api.app.dispatch import dispatch_celery_task

        user = _create_user(db_session)
        run = _create_queued_run(db_session, user)

        _mock_celery_module.send_task.side_effect = ConnectionError("broker down")

        dispatch_celery_task(
            db=db_session,
            job=run,
            task_name="backtests.run",
            task_kwargs={"run_id": str(run.id)},
            queue="research",
            log_event="backtest",
            logger=structlog.get_logger("test"),
        )

        outbox_msgs = list(db_session.scalars(
            select(OutboxMessage).where(OutboxMessage.correlation_id == run.id)
        ))
        assert len(outbox_msgs) == 1, "Exactly one OutboxMessage should be created"
        assert outbox_msgs[0].status == "pending"
        assert outbox_msgs[0].task_name == "backtests.run"

    def test_outbox_marked_sent_on_success(self, db_session, _mock_celery_module):
        """When inline send_task succeeds, the OutboxMessage should be 'sent'."""
        import types as _types
        import structlog
        from apps.api.app.dispatch import DispatchResult, dispatch_celery_task

        user = _create_user(db_session)
        run = _create_queued_run(db_session, user)

        _mock_celery_module.send_task.return_value = _types.SimpleNamespace(id="fake-task-id")

        result = dispatch_celery_task(
            db=db_session,
            job=run,
            task_name="backtests.run",
            task_kwargs={"run_id": str(run.id)},
            queue="research",
            log_event="backtest",
            logger=structlog.get_logger("test"),
        )

        assert result == DispatchResult.SENT

        outbox_msgs = list(db_session.scalars(
            select(OutboxMessage).where(OutboxMessage.correlation_id == run.id)
        ))
        assert len(outbox_msgs) == 1
        assert outbox_msgs[0].status == "sent"

    def test_idempotent_dispatch_skips_already_dispatched(self, db_session, _mock_celery_module):
        """Calling dispatch twice on the same job should skip the second call."""
        import types as _types
        import structlog
        from apps.api.app.dispatch import DispatchResult, dispatch_celery_task

        user = _create_user(db_session)
        run = _create_queued_run(db_session, user)

        _mock_celery_module.send_task.return_value = _types.SimpleNamespace(id="fake-task-id")

        first = dispatch_celery_task(
            db=db_session,
            job=run,
            task_name="backtests.run",
            task_kwargs={"run_id": str(run.id)},
            queue="research",
            log_event="backtest",
            logger=structlog.get_logger("test"),
        )
        assert first == DispatchResult.SENT

        db_session.refresh(run)
        second = dispatch_celery_task(
            db=db_session,
            job=run,
            task_name="backtests.run",
            task_kwargs={"run_id": str(run.id)},
            queue="research",
            log_event="backtest",
            logger=structlog.get_logger("test"),
        )
        assert second == DispatchResult.SKIPPED
