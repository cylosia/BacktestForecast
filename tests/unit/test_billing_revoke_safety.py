"""Verify billing cancellation revokes Celery tasks with terminate=False.

We use terminate=False so the worker can finish any in-progress cleanup
before exiting.  The DB status is already set to 'cancelled' before
revocation is issued.
"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import date
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.models import BacktestRun, SweepJob, User
from backtestforecast.services.billing import BillingService


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


def _make_session() -> Session:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    _strip_partial_indexes_for_sqlite(engine)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)()


def test_revoke_uses_terminate_false():
    session = _make_session()
    user = User(clerk_user_id="test_user", plan_tier="pro", subscription_status="active")
    session.add(user)
    session.flush()

    run = BacktestRun(
        user_id=user.id, status="running", symbol="AAPL", strategy_type="long_call",
        date_from=date(2025, 1, 1), date_to=date(2025, 6, 1), target_dte=30,
        dte_tolerance_days=5, max_holding_days=30, account_size=10000,
        risk_per_trade_pct=5, commission_per_contract=0.65, input_snapshot_json={},
        celery_task_id="task-123",
    )
    session.add(run)
    session.flush()

    mock_celery = MagicMock()
    svc = BillingService(session)

    with (
        patch("backtestforecast.events.publish_job_status"),
        patch.dict(
            "sys.modules",
            {"apps.worker.app.celery_app": MagicMock(celery_app=mock_celery)},
        ),
    ):
        svc.cancel_in_flight_jobs(user.id)

    for t in threading.enumerate():
        if t.name != threading.current_thread().name and t.daemon:
            t.join(timeout=2.0)

    assert mock_celery.control.revoke.call_count >= 1, "revoke should be called"
    for call in mock_celery.control.revoke.call_args_list:
        _, kwargs = call
        assert kwargs.get("terminate") is False, (
            f"revoke must use terminate=False, got: {kwargs}"
        )


def test_revoke_calls_terminate_false_in_source():
    """Ensure revoke uses terminate=False in source."""
    import inspect
    from backtestforecast.services.billing import BillingService

    source = inspect.getsource(BillingService.cancel_in_flight_jobs)
    assert "terminate=False" in source, (
        "cancel_in_flight_jobs should use terminate=False for revoke"
    )
