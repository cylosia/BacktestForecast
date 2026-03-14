"""Test: reaper double-dispatch scenario.

Verifies that the distributed lock prevents multiple reaper invocations
from redispatching the same stuck jobs simultaneously.
"""
from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.models import BacktestRun, User


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    yield session
    session.close()
    engine.dispose()


@pytest.fixture()
def db_session_factory(db_session):
    engine = db_session.get_bind()
    factory = sessionmaker(bind=engine)

    def create():
        return factory()
    return create


def test_reaper_skips_when_lock_already_held(monkeypatch):
    """When the Redis lock is already held, reaper returns skipped=1."""
    import apps.worker.app.tasks as tasks_module

    mock_redis_cls = MagicMock()
    mock_redis_inst = MagicMock()
    mock_lock = MagicMock()
    mock_lock.acquire.return_value = False
    mock_redis_inst.lock.return_value = mock_lock
    mock_redis_cls.from_url.return_value = mock_redis_inst

    with patch("backtestforecast.config.get_settings") as mock_settings, \
         patch("redis.Redis", mock_redis_cls):
        mock_settings.return_value = SimpleNamespace(redis_url="redis://localhost:6379/0")
        result = tasks_module.reap_stale_jobs(stale_minutes=30)

    assert result.get("skipped") == 1


def test_reaper_does_not_double_dispatch_same_run(db_session, db_session_factory, monkeypatch):
    """A stuck run should only be dispatched once, not duplicated."""
    user = User(
        id=uuid4(),
        clerk_user_id="user_reaper_test",
        email="reaper@example.com",
        plan_tier="pro",
        created_at=datetime.now(UTC),
    )
    db_session.add(user)
    db_session.commit()

    cutoff = datetime.now(UTC) - timedelta(minutes=60)
    stuck_run = BacktestRun(
        id=uuid4(),
        user_id=user.id,
        symbol="AAPL",
        strategy_type="long_call",
        status="queued",
        celery_task_id=None,
        created_at=cutoff,
        date_from=date(2024, 1, 1),
        date_to=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=7,
        max_holding_days=45,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("0.65"),
        input_snapshot_json={},
    )
    db_session.add(stuck_run)
    db_session.commit()

    dispatched: list[tuple[str, dict]] = []

    def fake_send_task(name, kwargs=None, **kw):
        dispatched.append((name, kwargs or {}))
        return SimpleNamespace(id="celery-redispatch")

    import apps.worker.app.tasks as tasks_module

    monkeypatch.setattr(tasks_module, "SessionLocal", db_session_factory)
    monkeypatch.setattr(tasks_module.celery_app, "send_task", fake_send_task)

    result = tasks_module._reap_stale_jobs_inner(stale_minutes=30)

    first_dispatch_count = len(dispatched)
    result2 = tasks_module._reap_stale_jobs_inner(stale_minutes=30)

    assert len(dispatched) == first_dispatch_count, (
        "Second reaper run should not dispatch the same job again"
    )


def test_reaper_runs_without_lock_when_redis_unavailable(db_session, db_session_factory, monkeypatch):
    """When Redis is unavailable, the reaper still runs (lock=None path)
    and still dispatches stale tasks. Without the lock, concurrent reapers
    could double-dispatch — the test demonstrates the code path proceeds."""
    user = User(
        id=uuid4(),
        clerk_user_id="user_redis_down",
        email="redisdown@example.com",
        plan_tier="pro",
        created_at=datetime.now(UTC),
    )
    db_session.add(user)
    db_session.commit()

    cutoff = datetime.now(UTC) - timedelta(minutes=60)
    stuck_run = BacktestRun(
        id=uuid4(),
        user_id=user.id,
        symbol="TSLA",
        strategy_type="long_call",
        status="queued",
        celery_task_id=None,
        created_at=cutoff,
        date_from=date(2024, 1, 1),
        date_to=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=7,
        max_holding_days=45,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("0.65"),
        input_snapshot_json={},
    )
    db_session.add(stuck_run)
    db_session.commit()

    dispatched: list[tuple[str, dict]] = []

    def fake_send_task(name, kwargs=None, **kw):
        dispatched.append((name, kwargs or {}))
        return SimpleNamespace(id="celery-redis-down")

    import apps.worker.app.tasks as tasks_module

    monkeypatch.setattr(tasks_module, "SessionLocal", db_session_factory)
    monkeypatch.setattr(tasks_module.celery_app, "send_task", fake_send_task)

    mock_redis_cls = MagicMock()
    mock_redis_cls.from_url.side_effect = ConnectionError("Redis is down")

    with patch("backtestforecast.config.get_settings") as mock_settings, \
         patch("redis.Redis", mock_redis_cls):
        mock_settings.return_value = SimpleNamespace(redis_url="redis://localhost:6379/0")
        result = tasks_module.reap_stale_jobs(stale_minutes=30)

    assert "backtest_runs" in result
    assert result["backtest_runs"] >= 1
    assert len(dispatched) >= 1, "Reaper should still dispatch even when Redis lock is unavailable"

    dispatched2: list[tuple[str, dict]] = []

    def fake_send_task2(name, kwargs=None, **kw):
        dispatched2.append((name, kwargs or {}))
        return SimpleNamespace(id="celery-redis-down-2")

    monkeypatch.setattr(tasks_module.celery_app, "send_task", fake_send_task2)

    db_session.expire_all()
    refreshed = db_session.get(BacktestRun, stuck_run.id)
    if refreshed.celery_task_id is not None:
        refreshed.celery_task_id = None
        refreshed.status = "queued"
        db_session.commit()

    with patch("backtestforecast.config.get_settings") as mock_settings, \
         patch("redis.Redis", mock_redis_cls):
        mock_settings.return_value = SimpleNamespace(redis_url="redis://localhost:6379/0")
        result2 = tasks_module.reap_stale_jobs(stale_minutes=30)

    assert result2["backtest_runs"] >= 1, (
        "Without Redis lock, a second reaper would also see and dispatch the same stale jobs"
    )
