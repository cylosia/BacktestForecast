"""Unit tests for Celery worker tasks."""
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
from backtestforecast.errors import AppError
from backtestforecast.models import BacktestRun, User

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
def db_session_factory(db_engine):
    return sessionmaker(bind=db_engine, autoflush=False, expire_on_commit=False)


@pytest.fixture()
def db_session(db_session_factory) -> Session:
    session = db_session_factory()
    try:
        yield session
    finally:
        session.close()


def _create_user(session: Session) -> User:
    user = User(clerk_user_id="test_worker_user", email="worker@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


# ---------------------------------------------------------------------------
# run_backtest
# ---------------------------------------------------------------------------


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_backtest_success(mock_session_local, mock_publish):
    from apps.worker.app.tasks import run_backtest

    mock_run = SimpleNamespace(status="succeeded", trade_count=5)
    mock_service = MagicMock()
    mock_service.execute_run_by_id.return_value = mock_run
    mock_service.close = MagicMock()

    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=MagicMock())
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with patch("apps.worker.app.tasks.BacktestService", return_value=mock_service):
        result = run_backtest(str(uuid4()))

    assert result["status"] == "succeeded"
    assert result["trade_count"] == 5
    mock_service.close.assert_called_once()
    assert mock_publish.call_count == 2
    assert mock_publish.call_args_list[0].args[2] == "running"
    assert mock_publish.call_args_list[1].args[2] == "succeeded"


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_backtest_app_error(mock_session_local, mock_publish):
    from apps.worker.app.tasks import run_backtest

    mock_service = MagicMock()
    mock_service.execute_run_by_id.side_effect = AppError("test_error", "Something broke")
    mock_service.close = MagicMock()

    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=MagicMock())
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with patch("apps.worker.app.tasks.BacktestService", return_value=mock_service):
        result = run_backtest(str(uuid4()))

    assert result["status"] == "failed"
    assert result["error_code"] == "test_error"
    mock_service.close.assert_called_once()


# ---------------------------------------------------------------------------
# run_scan_job
# ---------------------------------------------------------------------------


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_scan_job_success(mock_session_local, mock_publish):
    from apps.worker.app.tasks import run_scan_job

    mock_job = SimpleNamespace(status="succeeded", recommendation_count=3)
    mock_service = MagicMock()
    mock_service.run_job.return_value = mock_job
    mock_service.close = MagicMock()

    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=MagicMock())
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with patch("apps.worker.app.tasks.ScanService", return_value=mock_service):
        result = run_scan_job(str(uuid4()))

    assert result["status"] == "succeeded"
    assert result["recommendation_count"] == 3
    mock_service.close.assert_called_once()
    assert mock_publish.call_args_list[0].args[2] == "running"
    assert mock_publish.call_args_list[1].args[2] == "succeeded"


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_scan_job_app_error(mock_session_local, mock_publish):
    from apps.worker.app.tasks import run_scan_job

    mock_service = MagicMock()
    mock_service.run_job.side_effect = AppError("scan_error", "Scan broke")
    mock_service.close = MagicMock()

    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=MagicMock())
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with patch("apps.worker.app.tasks.ScanService", return_value=mock_service):
        result = run_scan_job(str(uuid4()))

    assert result["status"] == "failed"
    assert result["error_code"] == "scan_error"
    mock_service.close.assert_called_once()


# ---------------------------------------------------------------------------
# reap_stale_jobs
# ---------------------------------------------------------------------------


def test_reap_stale_jobs_redispatches(db_session, db_session_factory, monkeypatch):
    user = _create_user(db_session)

    stale_time = datetime.now(UTC) - timedelta(minutes=60)
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
    run.created_at = stale_time
    db_session.add(run)
    db_session.commit()
    db_session.refresh(run)
    run_id = run.id

    dispatched_tasks = []

    def fake_send_task(name, kwargs, **extra):
        dispatched_tasks.append((name, kwargs))
        return SimpleNamespace(id=f"celery-{name}")

    import apps.worker.app.tasks as tasks_module

    monkeypatch.setattr(tasks_module, "SessionLocal", db_session_factory)
    monkeypatch.setattr(tasks_module.celery_app, "send_task", fake_send_task)

    result = tasks_module.reap_stale_jobs(stale_minutes=30)

    assert result["backtest_runs"] == 1
    assert any("backtests.run" == t[0] for t in dispatched_tasks)

    db_session.expire_all()
    refreshed = db_session.get(BacktestRun, run_id)
    assert refreshed.celery_task_id is not None


def test_reap_stale_jobs_skips_dispatched(db_session, db_session_factory, monkeypatch):
    user = _create_user(db_session)

    stale_time = datetime.now(UTC) - timedelta(minutes=60)
    run = BacktestRun(
        user_id=user.id,
        symbol="AAPL",
        strategy_type="long_call",
        status="queued",
        celery_task_id="already-dispatched",
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
    run.created_at = stale_time
    db_session.add(run)
    db_session.commit()

    dispatched_tasks = []

    def fake_send_task(name, kwargs, **extra):
        dispatched_tasks.append((name, kwargs))
        return SimpleNamespace(id="celery-new")

    import apps.worker.app.tasks as tasks_module

    monkeypatch.setattr(tasks_module, "SessionLocal", db_session_factory)
    monkeypatch.setattr(tasks_module.celery_app, "send_task", fake_send_task)

    result = tasks_module.reap_stale_jobs(stale_minutes=30)

    assert result["backtest_runs"] == 0
    assert not any("backtests.run" == t[0] for t in dispatched_tasks)


def test_reap_stale_jobs_skips_recent(db_session, db_session_factory, monkeypatch):
    user = _create_user(db_session)

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
    db_session.add(run)
    db_session.commit()

    dispatched_tasks = []

    def fake_send_task(name, kwargs, **extra):
        dispatched_tasks.append((name, kwargs))
        return SimpleNamespace(id="celery-new")

    import apps.worker.app.tasks as tasks_module

    monkeypatch.setattr(tasks_module, "SessionLocal", db_session_factory)
    monkeypatch.setattr(tasks_module.celery_app, "send_task", fake_send_task)

    result = tasks_module.reap_stale_jobs(stale_minutes=30)

    assert result["backtest_runs"] == 0
    assert len(dispatched_tasks) == 0
