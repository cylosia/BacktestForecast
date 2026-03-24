from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.models import BacktestRun, ExportJob, ScannerJob, SweepJob, SymbolAnalysis, User
from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService
from backtestforecast.services.backtests import BacktestService
from backtestforecast.services.exports import ExportService
from backtestforecast.services.scans import ScanService
from backtestforecast.services.sweeps import SweepService
from tests.conftest import strip_partial_indexes_for_sqlite as _strip_partial_indexes_for_sqlite


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
    user = User(clerk_user_id="cancelled-worker-user", email="cancelled-worker@example.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_backtest_run(session: Session, user: User, *, status: str = "cancelled") -> BacktestRun:
    run = BacktestRun(
        user_id=user.id,
        status=status,
        symbol="AAPL",
        strategy_type="long_call",
        date_from=date(2024, 1, 2),
        date_to=date(2024, 3, 29),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        risk_free_rate=Decimal("0.0125"),
        input_snapshot_json={},
    )
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def _create_export_job(session: Session, user: User, run: BacktestRun, *, status: str = "cancelled") -> ExportJob:
    job = ExportJob(
        user_id=user.id,
        backtest_run_id=run.id,
        export_format="csv",
        status=status,
        file_name="run.csv",
        mime_type="text/csv",
        size_bytes=128 if status == "succeeded" else 0,
        sha256_hex="abc123" if status == "succeeded" else None,
        storage_key="exports/run.csv" if status == "succeeded" else None,
        completed_at=datetime.now(UTC) if status == "succeeded" else None,
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def _create_scan_job(session: Session, user: User, *, status: str = "cancelled") -> ScannerJob:
    job = ScannerJob(
        user_id=user.id,
        status=status,
        mode="basic",
        plan_tier_snapshot="pro",
        request_hash="scan-hash",
        request_snapshot_json={},
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def _create_sweep_job(session: Session, user: User, *, status: str = "cancelled") -> SweepJob:
    job = SweepJob(
        user_id=user.id,
        symbol="SPY",
        mode="grid",
        status=status,
        plan_tier_snapshot="pro",
        candidate_count=1,
        request_snapshot_json={},
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def _create_analysis(session: Session, user: User, *, status: str = "cancelled") -> SymbolAnalysis:
    analysis = SymbolAnalysis(
        user_id=user.id,
        symbol="AAPL",
        status=status,
        stage="pending",
    )
    session.add(analysis)
    session.commit()
    session.refresh(analysis)
    return analysis


def test_backtest_worker_skips_cancelled_run(db_session):
    user = _create_user(db_session)
    run = _create_backtest_run(db_session, user)

    result = BacktestService(db_session).execute_run_by_id(run.id)

    assert result.id == run.id
    assert result.status == "cancelled"


def test_export_worker_skips_cancelled_job(db_session):
    user = _create_user(db_session)
    run = _create_backtest_run(db_session, user)
    job = _create_export_job(db_session, user, run)

    result = ExportService(db_session).execute_export_by_id(job.id)

    assert result.id == job.id
    assert result.status == "cancelled"


def test_scan_worker_skips_cancelled_job(db_session):
    user = _create_user(db_session)
    job = _create_scan_job(db_session, user)

    result = ScanService(db_session).run_job(job.id)

    assert result.id == job.id
    assert result.status == "cancelled"


def test_sweep_worker_skips_cancelled_job(db_session):
    user = _create_user(db_session)
    job = _create_sweep_job(db_session, user)

    result = SweepService(db_session).run_job(job.id)

    assert result.id == job.id
    assert result.status == "cancelled"


def test_analysis_worker_skips_cancelled_job(db_session):
    user = _create_user(db_session)
    analysis = _create_analysis(db_session, user)

    result = SymbolDeepAnalysisService(db_session, market_data_fetcher=None, backtest_executor=None).execute_analysis(analysis.id)

    assert result.id == analysis.id
    assert result.status == "cancelled"


def test_backtest_worker_retry_is_idempotent_after_success(db_session):
    user = _create_user(db_session)
    run = _create_backtest_run(db_session, user, status="succeeded")

    mock_exec = MagicMock()
    service = BacktestService(db_session, execution_service=mock_exec)

    result = service.execute_run_by_id(run.id)

    assert result.id == run.id
    assert result.status == "succeeded"
    mock_exec.execute_request.assert_not_called()


def test_export_worker_retry_is_idempotent_after_success(db_session):
    user = _create_user(db_session)
    run = _create_backtest_run(db_session, user, status="succeeded")
    job = _create_export_job(db_session, user, run, status="succeeded")

    storage = MagicMock()
    service = ExportService(db_session, storage=storage)
    service.backtest_service = MagicMock()

    result = service.execute_export_by_id(job.id)

    assert result.id == job.id
    assert result.status == "succeeded"
    service.backtest_service.get_run_for_owner.assert_not_called()
    storage.put.assert_not_called()


def test_scan_worker_retry_is_idempotent_after_success(db_session):
    user = _create_user(db_session)
    job = _create_scan_job(db_session, user, status="succeeded")

    execution_service = MagicMock()
    service = ScanService(db_session, execution_service=execution_service)

    result = service.run_job(job.id)

    assert result.id == job.id
    assert result.status == "succeeded"
    execution_service.execute_request.assert_not_called()


def test_sweep_worker_retry_is_idempotent_after_success(db_session):
    user = _create_user(db_session)
    job = _create_sweep_job(db_session, user, status="succeeded")

    execution_service = MagicMock()
    service = SweepService(db_session, execution_service=execution_service)

    result = service.run_job(job.id)

    assert result.id == job.id
    assert result.status == "succeeded"
    execution_service.execute_request.assert_not_called()
