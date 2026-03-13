"""Test: reaper double-dispatch scenario.

Verifies that the distributed lock prevents multiple reaper invocations
from redispatching the same stuck jobs simultaneously.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
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
    def factory():
        return db_session
    return factory


def test_reaper_skips_when_lock_already_held(monkeypatch):
    """When the Redis lock is already held, reaper returns skipped=1."""
    import apps.worker.app.tasks as tasks_module

    mock_redis_cls = MagicMock()
    mock_redis_inst = MagicMock()
    mock_lock = MagicMock()
    mock_lock.acquire.return_value = False
    mock_redis_inst.lock.return_value = mock_lock
    mock_redis_cls.from_url.return_value = mock_redis_inst

    with patch("apps.worker.app.tasks.get_settings") as mock_settings:
        mock_settings.return_value = SimpleNamespace(redis_url="redis://localhost:6379/0")
        with patch.dict("sys.modules", {"redis": MagicMock(Redis=mock_redis_cls)}):
            with patch("apps.worker.app.tasks.Redis", mock_redis_cls):
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
        strategy_type="long_only",
        status="queued",
        celery_task_id=None,
        created_at=cutoff,
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
