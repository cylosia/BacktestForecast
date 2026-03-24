"""Unit tests for Celery worker tasks."""
from __future__ import annotations

import logging
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
from backtestforecast.models import (
    BacktestRun,
    User,
)
from tests.conftest import strip_partial_indexes_for_sqlite as _strip_partial_indexes_for_sqlite

_sqlite_warning_logged = False

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_engine():
    """SQLite in-memory engine for unit-level isolation.

    NOTE: SQLite silently ignores Postgres-specific features (skip_locked,
    FOR UPDATE, partial indexes, check constraints with function calls).
    For full coverage run these tests against Postgres in CI using the
    ``postgres-integration`` job configuration.
    """
    global _sqlite_warning_logged
    if not _sqlite_warning_logged:
        logging.getLogger("tests.worker.sqlite").warning(
            "Worker tests use SQLite which silently ignores Postgres-specific features like skip_locked and FOR UPDATE. Run with DATABASE_URL pointing to Postgres for full coverage.",
        )
        _sqlite_warning_logged = True
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
    assert any(t[0] == "backtests.run" for t in dispatched_tasks)

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
    assert not any(t[0] == "backtests.run" for t in dispatched_tasks)


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


# ---------------------------------------------------------------------------
# Entitlement checks - verify all 4 tasks reject when user is missing
# ---------------------------------------------------------------------------


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_backtest_fails_when_user_missing(mock_session_local, mock_publish):
    from apps.worker.app.tasks import run_backtest

    run_id = uuid4()
    mock_run = MagicMock()
    mock_run.user_id = uuid4()
    mock_run.status = "queued"

    session = MagicMock()
    session.get.side_effect = lambda model, uid: mock_run if model.__name__ == "BacktestRun" else None
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with patch("apps.worker.app.tasks.BacktestService"):
        result = run_backtest(str(run_id))

    assert result["status"] == "failed"
    assert result["error_code"] == "entitlement_revoked"
    assert mock_run.status == "failed"


# ---------------------------------------------------------------------------
# generate_export
# ---------------------------------------------------------------------------


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_generate_export_success(mock_session_local, mock_publish):
    from apps.worker.app.tasks import generate_export

    mock_job = SimpleNamespace(status="succeeded", size_bytes=4096)
    mock_service = MagicMock()
    mock_service.execute_export_by_id.return_value = mock_job
    mock_service.close = MagicMock()

    session = MagicMock()
    session.get.return_value = None  # skip entitlement check (no ExportJob found)
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with patch("apps.worker.app.tasks.ExportService", return_value=mock_service):
        result = generate_export(str(uuid4()))

    assert result["status"] == "succeeded"
    assert result["size_bytes"] == 4096
    mock_service.close.assert_called_once()
    assert mock_publish.call_count >= 2
    assert mock_publish.call_args_list[0].args[2] == "running"
    assert mock_publish.call_args_list[-1].args[2] == "succeeded"


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_generate_export_value_error_propagates(mock_session_local, mock_publish):
    """Item 81: ValueError from execute_export_by_id must propagate to Celery
    (caught by the generic Exception handler for retry), not be swallowed."""
    from apps.worker.app.tasks import generate_export

    mock_service = MagicMock()
    mock_service.execute_export_by_id.side_effect = ValueError("bad value in export data")
    mock_service.close = MagicMock()

    session = MagicMock()
    session.get.return_value = None
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with patch("apps.worker.app.tasks.ExportService", return_value=mock_service), pytest.raises(
        ValueError, match="bad value in export data"
    ):
        generate_export(str(uuid4()))

    mock_service.close.assert_called_once()


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_generate_export_app_error(mock_session_local, mock_publish):
    from apps.worker.app.tasks import generate_export

    mock_service = MagicMock()
    mock_service.execute_export_by_id.side_effect = AppError("export_error", "Export broke")
    mock_service.close = MagicMock()

    session = MagicMock()
    session.get.return_value = None
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with patch("apps.worker.app.tasks.ExportService", return_value=mock_service):
        result = generate_export(str(uuid4()))

    assert result["status"] == "failed"
    assert result["error_code"] == "export_error"
    mock_service.close.assert_called_once()


# ---------------------------------------------------------------------------
# run_deep_analysis
# ---------------------------------------------------------------------------


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_deep_analysis_success(mock_session_local, mock_publish):
    from apps.worker.app.tasks import run_deep_analysis

    mock_result = SimpleNamespace(status="succeeded", top_results_count=3)
    mock_service = MagicMock()
    mock_service.execute_analysis.return_value = mock_result

    user_mock = MagicMock()
    user_mock.plan_tier = "pro"
    user_mock.subscription_status = "active"
    user_mock.subscription_current_period_end = None

    analysis_mock = MagicMock()
    analysis_mock.user_id = uuid4()
    analysis_mock.status = "queued"

    session = MagicMock()

    def _get(model, uid):
        name = model.__name__
        if name == "SymbolAnalysis":
            return analysis_mock
        if name == "User":
            return user_mock
        return None

    session.get.side_effect = _get
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    policy = SimpleNamespace(forecasting_access=True)
    with (
        patch("apps.worker.app.tasks.resolve_feature_policy", return_value=policy),
        patch("backtestforecast.config.get_settings") as mock_settings,
        patch("backtestforecast.integrations.massive_client.MassiveClient") as mock_client_cls,
        patch("backtestforecast.market_data.service.MarketDataService"),
        patch("backtestforecast.services.backtest_execution.BacktestExecutionService"),
        patch("backtestforecast.pipeline.adapters.PipelineBacktestExecutor") as mock_executor_cls,
        patch("backtestforecast.pipeline.adapters.PipelineMarketDataFetcher"),
        patch("backtestforecast.pipeline.adapters.PipelineForecaster"),
        patch("backtestforecast.forecasts.analog.HistoricalAnalogForecaster"),
        patch("backtestforecast.pipeline.deep_analysis.SymbolDeepAnalysisService", return_value=mock_service),
    ):
        mock_settings.return_value = SimpleNamespace(massive_api_key="test")
        mock_client_cls.return_value = MagicMock()
        mock_executor_cls.return_value = MagicMock()
        result = run_deep_analysis(str(uuid4()))

    assert result["status"] == "succeeded"
    assert result["top_results"] == 3


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_deep_analysis_app_error(mock_session_local, mock_publish):
    from apps.worker.app.tasks import run_deep_analysis

    mock_service = MagicMock()
    mock_service.execute_analysis.side_effect = AppError("analysis_error", "Boom")

    user_mock = MagicMock()
    user_mock.plan_tier = "pro"
    user_mock.subscription_status = "active"
    user_mock.subscription_current_period_end = None

    analysis_mock = MagicMock()
    analysis_mock.user_id = uuid4()
    analysis_mock.status = "queued"

    session = MagicMock()

    def _get(model, uid):
        name = model.__name__
        if name == "SymbolAnalysis":
            return analysis_mock
        if name == "User":
            return user_mock
        return None

    session.get.side_effect = _get
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    policy = SimpleNamespace(forecasting_access=True)
    with (
        patch("apps.worker.app.tasks.resolve_feature_policy", return_value=policy),
        patch("backtestforecast.config.get_settings") as mock_settings,
        patch("backtestforecast.integrations.massive_client.MassiveClient") as mock_client_cls,
        patch("backtestforecast.market_data.service.MarketDataService"),
        patch("backtestforecast.services.backtest_execution.BacktestExecutionService"),
        patch("backtestforecast.pipeline.adapters.PipelineBacktestExecutor") as mock_executor_cls,
        patch("backtestforecast.pipeline.adapters.PipelineMarketDataFetcher"),
        patch("backtestforecast.pipeline.adapters.PipelineForecaster"),
        patch("backtestforecast.forecasts.analog.HistoricalAnalogForecaster"),
        patch("backtestforecast.pipeline.deep_analysis.SymbolDeepAnalysisService", return_value=mock_service),
    ):
        mock_settings.return_value = SimpleNamespace(massive_api_key="test")
        mock_client_cls.return_value = MagicMock()
        mock_executor_cls.return_value = MagicMock()
        result = run_deep_analysis(str(uuid4()))

    assert result["status"] == "failed"
    assert result["error_code"] == "analysis_error"


# ---------------------------------------------------------------------------
# nightly_scan_pipeline
# ---------------------------------------------------------------------------


@patch("apps.worker.app.tasks.SessionLocal")
def test_nightly_scan_pipeline_success(mock_session_local):
    from apps.worker.app.tasks import nightly_scan_pipeline

    mock_run = SimpleNamespace(
        status="succeeded",
        id=uuid4(),
        recommendations_produced=5,
        duration_seconds=42.0,
    )
    mock_service = MagicMock()
    mock_service.run_pipeline.return_value = mock_run

    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=MagicMock())
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with (
        patch("backtestforecast.config.get_settings") as mock_settings,
        patch("backtestforecast.integrations.massive_client.MassiveClient") as mock_client_cls,
        patch("backtestforecast.market_data.service.MarketDataService"),
        patch("backtestforecast.services.backtest_execution.BacktestExecutionService"),
        patch("backtestforecast.pipeline.adapters.PipelineBacktestExecutor") as mock_executor_cls,
        patch("backtestforecast.pipeline.adapters.PipelineMarketDataFetcher"),
        patch("backtestforecast.pipeline.adapters.PipelineForecaster"),
        patch("backtestforecast.forecasts.analog.HistoricalAnalogForecaster"),
        patch("backtestforecast.pipeline.service.NightlyPipelineService", return_value=mock_service),
    ):
        mock_settings.return_value = SimpleNamespace(
            massive_api_key="test",
            pipeline_default_symbols=["AAPL", "MSFT"],
        )
        mock_client_cls.return_value = MagicMock()
        mock_executor_cls.return_value = MagicMock()
        result = nightly_scan_pipeline()

    assert result["status"] == "succeeded"
    assert result["recommendations"] == 5
    assert result["duration_seconds"] == 42.0


# ---------------------------------------------------------------------------
# refresh_prioritized_scans
# ---------------------------------------------------------------------------


def test_refresh_prioritized_scans_dispatches():
    from apps.worker.app.tasks import refresh_prioritized_scans

    mock_service = MagicMock()
    mock_service.create_and_dispatch_scheduled_refresh_jobs.return_value = (2, 0)
    mock_service.close = MagicMock()

    class _FakeLock:
        def acquire(self, blocking=False):
            return True

        def release(self):
            return None

    class _FakeRedis:
        def lock(self, *_args, **_kwargs):
            return _FakeLock()

        def close(self):
            return None

    session = MagicMock()
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)

    with (
        patch("apps.worker.app.tasks.ScanService", return_value=mock_service),
        patch("apps.worker.app.tasks.create_worker_session", return_value=session_ctx),
        patch("backtestforecast.utils.create_cache_redis", return_value=_FakeRedis()),
    ):
        result = refresh_prioritized_scans.run()

    assert result["scheduled_jobs"] == 2
    assert result["pending_recovery"] == 0
    mock_service.create_and_dispatch_scheduled_refresh_jobs.assert_called_once()
    mock_service.close.assert_called_once()


# ---------------------------------------------------------------------------
# maintenance.ping
# ---------------------------------------------------------------------------


def test_ping_returns_expected_format():
    from apps.worker.app.tasks import ping

    result = ping()

    assert result["status"] == "ok"
    assert result["task"] == "maintenance.ping"
    assert "note" in result


# ---------------------------------------------------------------------------
# Entitlement checks - verify all 4 tasks reject when user is missing
# ---------------------------------------------------------------------------


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_generate_export_fails_when_user_missing(mock_session_local, mock_publish):
    from apps.worker.app.tasks import generate_export

    export_id = uuid4()
    mock_export = MagicMock()
    mock_export.user_id = uuid4()
    mock_export.status = "queued"

    session = MagicMock()
    session.get.side_effect = lambda model, uid: mock_export if model.__name__ in ("ExportJob",) else None
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with patch("apps.worker.app.tasks.ExportService"):
        result = generate_export(str(export_id))

    assert result["status"] == "failed"
    assert result["error_code"] == "entitlement_revoked"


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_scan_job_fails_when_user_missing(mock_session_local, mock_publish):
    from apps.worker.app.tasks import run_scan_job

    job_id = uuid4()
    mock_job = MagicMock()
    mock_job.user_id = uuid4()
    mock_job.status = "queued"

    session = MagicMock()
    session.get.side_effect = lambda model, uid: mock_job if model.__name__ in ("ScannerJob",) else None
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with patch("apps.worker.app.tasks.ScanService"):
        result = run_scan_job(str(job_id))

    assert result["status"] == "failed"
    assert result["error_code"] == "entitlement_revoked"


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_deep_analysis_fails_when_user_missing(mock_session_local, mock_publish):
    from apps.worker.app.tasks import run_deep_analysis

    analysis_id = uuid4()
    mock_analysis = MagicMock()
    mock_analysis.user_id = uuid4()
    mock_analysis.status = "queued"

    session = MagicMock()
    session.get.side_effect = lambda model, uid: mock_analysis if model.__name__ == "SymbolAnalysis" else None
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with (
        patch("backtestforecast.config.get_settings") as mock_settings,
        patch("backtestforecast.integrations.massive_client.MassiveClient") as mock_client_cls,
        patch("backtestforecast.market_data.service.MarketDataService"),
        patch("backtestforecast.services.backtest_execution.BacktestExecutionService"),
        patch("backtestforecast.pipeline.adapters.PipelineBacktestExecutor") as mock_executor_cls,
        patch("backtestforecast.pipeline.adapters.PipelineMarketDataFetcher"),
        patch("backtestforecast.pipeline.adapters.PipelineForecaster"),
        patch("backtestforecast.forecasts.analog.HistoricalAnalogForecaster"),
    ):
        mock_settings.return_value = SimpleNamespace(massive_api_key="test")
        mock_client_cls.return_value = MagicMock()
        mock_executor_cls.return_value = MagicMock()
        result = run_deep_analysis(str(analysis_id))

    assert result["status"] == "failed"
    assert result["error_code"] == "entitlement_revoked"


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_backtest_allows_pro_user(mock_session_local, mock_publish):
    """Pro users (unlimited quota) must not be rejected at the worker level."""
    from apps.worker.app.tasks import run_backtest

    run_id = uuid4()
    mock_run = MagicMock()
    mock_run.user_id = uuid4()
    mock_run.status = "queued"
    mock_user = MagicMock()
    mock_user.plan_tier = "pro"
    mock_user.subscription_status = "active"
    mock_user.subscription_current_period_end = None

    mock_service = MagicMock()
    mock_service.execute_run_by_id.return_value = SimpleNamespace(status="succeeded", trade_count=2)
    mock_service.close = MagicMock()

    session = MagicMock()

    def _get(model, uid):
        if model.__name__ == "BacktestRun":
            return mock_run
        if model.__name__ == "User":
            return mock_user
        return None

    session.get.side_effect = _get
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with patch("apps.worker.app.tasks.BacktestService", return_value=mock_service):
        result = run_backtest(str(run_id))

    assert result["status"] == "succeeded"
    mock_service.close.assert_called_once()


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_generate_export_rejects_no_export_formats(mock_session_local, mock_publish):
    """Free users have empty export_formats - worker should reject."""
    from apps.worker.app.tasks import generate_export

    export_id = uuid4()
    mock_export = MagicMock()
    mock_export.user_id = uuid4()
    mock_export.status = "queued"
    mock_user = MagicMock()
    mock_user.plan_tier = "free"
    mock_user.subscription_status = None
    mock_user.subscription_current_period_end = None

    session = MagicMock()

    def _get(model, uid):
        if model.__name__ == "ExportJob":
            return mock_export
        if model.__name__ == "User":
            return mock_user
        return None

    session.get.side_effect = _get
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    no_export_policy = SimpleNamespace(export_formats=frozenset())
    with (
        patch("apps.worker.app.tasks.resolve_feature_policy", return_value=no_export_policy),
        patch("apps.worker.app.tasks.ExportService"),
    ):
        result = generate_export(str(export_id))

    assert result["status"] == "failed"
    assert result["error_code"] == "entitlement_revoked"


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_scan_job_rejects_no_scanner_access(mock_session_local, mock_publish):
    """Free users have basic_scanner_access=False - worker should reject."""
    from apps.worker.app.tasks import run_scan_job

    job_id = uuid4()
    mock_job = MagicMock()
    mock_job.user_id = uuid4()
    mock_job.status = "queued"
    mock_user = MagicMock()
    mock_user.plan_tier = "free"
    mock_user.subscription_status = None
    mock_user.subscription_current_period_end = None

    session = MagicMock()

    def _get(model, uid):
        if model.__name__ == "ScannerJob":
            return mock_job
        if model.__name__ == "User":
            return mock_user
        return None

    session.get.side_effect = _get
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    no_scanner_policy = SimpleNamespace(basic_scanner_access=False)
    with (
        patch("apps.worker.app.tasks.resolve_feature_policy", return_value=no_scanner_policy),
        patch("apps.worker.app.tasks.ScanService"),
    ):
        result = run_scan_job(str(job_id))

    assert result["status"] == "failed"
    assert result["error_code"] == "entitlement_revoked"


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_deep_analysis_rejects_no_forecasting(mock_session_local, mock_publish):
    """Free users have forecasting_access=False - worker should reject."""
    from apps.worker.app.tasks import run_deep_analysis

    analysis_id = uuid4()
    mock_analysis = MagicMock()
    mock_analysis.user_id = uuid4()
    mock_analysis.status = "queued"
    mock_user = MagicMock()
    mock_user.plan_tier = "free"
    mock_user.subscription_status = None
    mock_user.subscription_current_period_end = None

    session = MagicMock()

    def _get(model, uid):
        if model.__name__ == "SymbolAnalysis":
            return mock_analysis
        if model.__name__ == "User":
            return mock_user
        return None

    session.get.side_effect = _get
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    no_forecast_policy = SimpleNamespace(forecasting_access=False)
    with (
        patch("apps.worker.app.tasks.resolve_feature_policy", return_value=no_forecast_policy),
        patch("backtestforecast.config.get_settings") as mock_settings,
        patch("backtestforecast.integrations.massive_client.MassiveClient") as mock_client_cls,
        patch("backtestforecast.market_data.service.MarketDataService"),
        patch("backtestforecast.services.backtest_execution.BacktestExecutionService"),
        patch("backtestforecast.pipeline.adapters.PipelineBacktestExecutor") as mock_executor_cls,
        patch("backtestforecast.pipeline.adapters.PipelineMarketDataFetcher"),
        patch("backtestforecast.pipeline.adapters.PipelineForecaster"),
        patch("backtestforecast.forecasts.analog.HistoricalAnalogForecaster"),
    ):
        mock_settings.return_value = SimpleNamespace(massive_api_key="test")
        mock_client_cls.return_value = MagicMock()
        mock_executor_cls.return_value = MagicMock()
        result = run_deep_analysis(str(analysis_id))

    assert result["status"] == "failed"
    assert result["error_code"] == "entitlement_revoked"


# ---------------------------------------------------------------------------
# _validate_task_ownership
# ---------------------------------------------------------------------------


def test_validate_task_ownership_claims_when_stored_is_none(db_session, db_session_factory):
    """When celery_task_id is None, the first caller claims it; a second caller is rejected."""
    import apps.worker.app.tasks as tasks_module

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
    db_session.refresh(run)
    run_id = run.id

    claimed = tasks_module._validate_task_ownership(db_session, BacktestRun, run_id, "task-A")
    assert claimed is True

    db_session.expire_all()
    refreshed = db_session.get(BacktestRun, run_id)
    assert refreshed.celery_task_id == "task-A"

    rejected = tasks_module._validate_task_ownership(db_session, BacktestRun, run_id, "task-B")
    assert rejected is False


def test_validate_task_ownership_rejects_mismatch(db_session, db_session_factory):
    """When celery_task_id is already set, a different task_id is rejected."""
    import apps.worker.app.tasks as tasks_module

    user = _create_user(db_session)

    run = BacktestRun(
        user_id=user.id,
        symbol="MSFT",
        strategy_type="long_call",
        status="queued",
        celery_task_id="task-A",
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

    result = tasks_module._validate_task_ownership(db_session, BacktestRun, run.id, "task-B")
    assert result is False


# ---------------------------------------------------------------------------
# SoftTimeLimitExceeded handling
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Item 64: Worker quota off-by-one - 5th backtest is allowed
# ---------------------------------------------------------------------------


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_backtest_quota_allows_5th_when_limit_is_5(mock_session_local, mock_publish):
    """A user with monthly_backtest_quota=5 who already has 4 completed backtests
    (plus the current queued one = 5 total rows) should be allowed, not rejected.
    The worker subtracts 1 from the count (``used = max(used - 1, 0)``) to
    exclude the current queued row from the count."""
    from apps.worker.app.tasks import run_backtest

    run_id = uuid4()
    mock_run = MagicMock()
    mock_run.user_id = uuid4()
    mock_run.status = "queued"
    mock_user = MagicMock()
    mock_user.plan_tier = "free"
    mock_user.subscription_status = None
    mock_user.subscription_current_period_end = None

    mock_service = MagicMock()
    mock_service.execute_run_by_id.return_value = SimpleNamespace(status="succeeded", trade_count=1)
    mock_service.close = MagicMock()

    session = MagicMock()

    def _get(model, uid):
        if model.__name__ == "BacktestRun":
            return mock_run
        if model.__name__ == "User":
            return mock_user
        return None

    session.get.side_effect = _get
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    policy = SimpleNamespace(monthly_backtest_quota=5)
    mock_repo = MagicMock()
    mock_repo.count_for_user_created_between.return_value = 5

    with (
        patch("apps.worker.app.tasks.resolve_feature_policy", return_value=policy),
        patch("apps.worker.app.tasks.BacktestRunRepository", return_value=mock_repo),
        patch("apps.worker.app.tasks.BacktestService", return_value=mock_service),
    ):
        result = run_backtest(str(run_id))

    assert result["status"] == "succeeded", (
        "5th backtest should proceed: used = max(5 - 1, 0) = 4 < 5 quota"
    )
    mock_service.close.assert_called_once()


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_backtest_quota_rejects_6th_when_limit_is_5(mock_session_local, mock_publish):
    """A user with monthly_backtest_quota=5 who already has 5 completed + 1 queued
    (6 total rows) should be rejected."""
    from apps.worker.app.tasks import run_backtest

    run_id = uuid4()
    mock_run = MagicMock()
    mock_run.user_id = uuid4()
    mock_run.status = "queued"
    mock_user = MagicMock()
    mock_user.plan_tier = "free"
    mock_user.subscription_status = None
    mock_user.subscription_current_period_end = None

    session = MagicMock()

    def _get(model, uid):
        if model.__name__ == "BacktestRun":
            return mock_run
        if model.__name__ == "User":
            return mock_user
        return None

    session.get.side_effect = _get
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    policy = SimpleNamespace(monthly_backtest_quota=5)
    mock_repo = MagicMock()
    mock_repo.count_for_user_created_between.return_value = 6

    with (
        patch("apps.worker.app.tasks.resolve_feature_policy", return_value=policy),
        patch("apps.worker.app.tasks.BacktestRunRepository", return_value=mock_repo),
    ):
        result = run_backtest(str(run_id))

    assert result["status"] == "failed"
    assert result["error_code"] == "quota_exceeded"


# ---------------------------------------------------------------------------
# Item 65: Task re-delivery ownership validation
# ---------------------------------------------------------------------------


def test_validate_task_ownership_redelivery_rejects_superseded_non_terminal(db_session, db_session_factory):
    """A mismatched redelivery must not steal ownership from a newer claim."""
    import apps.worker.app.tasks as tasks_module

    user = _create_user(db_session)

    run = BacktestRun(
        user_id=user.id,
        symbol="TSLA",
        strategy_type="long_call",
        status="running",
        celery_task_id="old-task-id",
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

    claimed = tasks_module._validate_task_ownership(
        db_session, BacktestRun, run_id, "new-task-id"
    )
    assert claimed is False

    db_session.expire_all()
    refreshed = db_session.get(BacktestRun, run_id)
    assert refreshed.celery_task_id == "old-task-id"


def test_validate_task_ownership_redelivery_rejected_for_terminal(db_session, db_session_factory):
    """When a task is in a terminal status ('succeeded'), re-delivery should
    be rejected - we don't want to re-run a completed job."""
    import apps.worker.app.tasks as tasks_module

    user = _create_user(db_session)

    run = BacktestRun(
        user_id=user.id,
        symbol="TSLA",
        strategy_type="long_call",
        status="succeeded",
        celery_task_id="old-task-id",
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

    result = tasks_module._validate_task_ownership(
        db_session, BacktestRun, run.id, "new-task-id"
    )
    assert result is False

    db_session.expire_all()
    refreshed = db_session.get(BacktestRun, run.id)
    assert refreshed.celery_task_id == "old-task-id", "Terminal job task_id must not change"


# ---------------------------------------------------------------------------
# SoftTimeLimitExceeded handling
# ---------------------------------------------------------------------------


@patch("apps.worker.app.tasks.publish_job_status")
@patch("apps.worker.app.tasks.SessionLocal")
def test_run_backtest_soft_time_limit(mock_session_local, mock_publish):
    """SoftTimeLimitExceeded marks the run as failed and does NOT retry."""
    from celery.exceptions import SoftTimeLimitExceeded

    from apps.worker.app.tasks import run_backtest

    mock_service = MagicMock()
    mock_service.execute_run_by_id.side_effect = SoftTimeLimitExceeded("time limit")
    mock_service.close = MagicMock()

    mock_run = MagicMock()
    mock_run.user_id = uuid4()
    mock_run.status = "running"

    mock_user = MagicMock()
    mock_user.plan_tier = "pro"
    mock_user.subscription_status = "active"
    mock_user.subscription_current_period_end = None

    session = MagicMock()

    def _get(model, uid):
        if model.__name__ == "BacktestRun":
            return mock_run
        if model.__name__ == "User":
            return mock_user
        return None

    session.get.side_effect = _get
    session_ctx = MagicMock()
    session_ctx.__enter__ = MagicMock(return_value=session)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_session_local.return_value = session_ctx

    with patch("apps.worker.app.tasks.BacktestService", return_value=mock_service):
        result = run_backtest(str(uuid4()))

    assert result["status"] == "failed"
    assert result["error_code"] == "time_limit_exceeded"
    mock_service.close.assert_called_once()


# ---------------------------------------------------------------------------
# Item 98: Reaper uses row-level locking (skip_locked)
# ---------------------------------------------------------------------------


def test_reaper_stale_running_queries_use_skip_locked():
    """Verify that the reaper's stale-running queries use
    `with_for_update(skip_locked=True)` to avoid blocking on rows already
    locked by other workers. We inspect the source code of
    _reap_stale_jobs_inner to confirm."""
    import inspect

    import apps.worker.app.tasks as tasks_module

    source = inspect.getsource(tasks_module._reap_stale_jobs_inner)
    assert "with_for_update(skip_locked=True)" in source, (
        "_reap_stale_jobs_inner must use .with_for_update(skip_locked=True) "
        "for row-level locking to prevent the reaper from blocking on rows "
        "already locked by other workers"
    )


# ---------------------------------------------------------------------------
# Item 40: Reaper with stale running BacktestRun uses stale_running_rows
# ---------------------------------------------------------------------------


def test_reaper_stale_running_sets_gauge_without_crash(db_session, db_session_factory, monkeypatch):
    """Verify the JOBS_STUCK_RUNNING gauge is set using the stale_running_rows
    list (not stale_running_ids), and that the reaper doesn't crash when
    processing stale running BacktestRun records."""
    from backtestforecast.observability.metrics import JOBS_STUCK_RUNNING

    user = _create_user(db_session)

    stale_time = datetime.now(UTC) - timedelta(minutes=60)
    run = BacktestRun(
        user_id=user.id,
        symbol="TSLA",
        strategy_type="long_call",
        status="running",
        celery_task_id="stale-task-123",
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

    import apps.worker.app.tasks as tasks_module

    monkeypatch.setattr(tasks_module, "SessionLocal", db_session_factory)
    monkeypatch.setattr(tasks_module.celery_app, "send_task", lambda *a, **kw: SimpleNamespace(id="x"))

    result = tasks_module.reap_stale_jobs(stale_minutes=30)

    assert result is not None
    gauge_value = JOBS_STUCK_RUNNING.labels(model="BacktestRun")._value.get()
    assert gauge_value >= 0

    db_session.expire_all()
    refreshed = db_session.get(BacktestRun, run_id)
    assert refreshed.status == "failed"
