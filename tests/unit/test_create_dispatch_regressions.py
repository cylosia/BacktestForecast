from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.billing.entitlements import ExportFormat
from backtestforecast.db.base import Base
from backtestforecast.models import BacktestRun, OutboxMessage, ScannerJob, User
from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService
from backtestforecast.schemas.analysis import CreateAnalysisRequest
from backtestforecast.schemas.backtests import CreateBacktestRunRequest
from backtestforecast.schemas.exports import CreateExportRequest
from backtestforecast.schemas.scans import CreateScannerJobRequest
from backtestforecast.schemas.sweeps import CreateSweepRequest
from backtestforecast.services.backtests import BacktestService
from backtestforecast.services.dispatch_recovery import repair_stranded_jobs
from backtestforecast.services.exports import ExportService
from backtestforecast.services.scans import ScanService
from backtestforecast.services.sweeps import SweepService
from tests.conftest import strip_partial_indexes_for_sqlite as _strip_partial_indexes_for_sqlite

UTC = UTC


@pytest.fixture(autouse=True)
def _mock_celery_module(monkeypatch):
    """Preload a fake celery module so dispatch imports stay isolated in unit tests."""
    import sys
    import types

    mock_celery = MagicMock()
    mock_module = types.ModuleType("apps.worker.app.celery_app")
    mock_module.celery_app = mock_celery
    monkeypatch.setitem(sys.modules, "apps.worker.app.celery_app", mock_module)
    return mock_celery


@pytest.fixture()
def db_session():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
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


def _create_user(session: Session, *, plan_tier: str = "premium") -> User:
    user = User(
        clerk_user_id=f"{plan_tier}-dispatch-user",
        email=f"{plan_tier}@dispatch.test",
        plan_tier=plan_tier,
        subscription_status="active",
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_succeeded_backtest(session: Session, user_id: UUID) -> BacktestRun:
    run = BacktestRun(
        user_id=user_id,
        status="succeeded",
        symbol="AAPL",
        strategy_type="long_call",
        date_from=date(2024, 1, 1),
        date_to=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        input_snapshot_json={},
    )
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def _assert_pending_outbox(session: Session, job_id: UUID, *, task_name: str, model_type):
    session.expire_all()
    job = session.get(model_type, job_id)
    assert job is not None
    assert job.status == "queued"
    assert job.celery_task_id is not None

    outbox_messages = list(
        session.scalars(
            select(OutboxMessage).where(OutboxMessage.correlation_id == job_id)
        )
    )
    assert len(outbox_messages) == 1
    assert outbox_messages[0].status == "pending"
    assert outbox_messages[0].task_name == task_name


def _assert_sent_outbox(session: Session, job_id: UUID, *, task_name: str, model_type):
    session.expire_all()
    job = session.get(model_type, job_id)
    assert job is not None
    assert job.status == "queued"
    assert job.celery_task_id is not None

    outbox_messages = list(
        session.scalars(
            select(OutboxMessage).where(OutboxMessage.correlation_id == job_id)
        )
    )
    assert len(outbox_messages) == 1
    assert outbox_messages[0].status == "sent"
    assert outbox_messages[0].task_name == task_name


def _mark_job_stale(session: Session, job, *, stale_task_id: str = "stale-task-id") -> None:
    stale_time = datetime.now(UTC) - timedelta(minutes=30)
    job.created_at = stale_time
    job.celery_task_id = stale_task_id
    session.add(
        OutboxMessage(
            task_name="stale.task",
            task_kwargs_json={"job_id": str(job.id)},
            queue="research",
            status="pending",
            correlation_id=job.id,
        )
    )
    session.commit()
    session.refresh(job)


def _assert_stale_job_redispatched(session: Session, job_id: UUID, *, task_name: str, model_type) -> None:
    session.expire_all()
    job = session.get(model_type, job_id)
    assert job is not None
    assert job.status == "queued"
    assert job.celery_task_id is not None
    assert job.celery_task_id != "stale-task-id"

    statuses = list(
        session.scalars(
            select(OutboxMessage.status)
            .where(
                OutboxMessage.correlation_id == job_id,
                OutboxMessage.task_name == task_name,
            )
            .order_by(OutboxMessage.created_at)
        )
    )
    assert statuses == ["sent"]


def test_backtest_create_and_dispatch_preserves_pending_outbox_on_send_failure(
    db_session: Session,
    _mock_celery_module: MagicMock,
) -> None:
    user = _create_user(db_session, plan_tier="pro")
    service = BacktestService(db_session)
    payload = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("40"), "period": 14}],
    )
    _mock_celery_module.send_task.side_effect = ConnectionError("broker down")

    run = service.create_and_dispatch(user, payload)

    _assert_pending_outbox(db_session, run.id, task_name="backtests.run", model_type=BacktestRun)


def test_backtest_create_and_dispatch_records_sent_outbox_on_success(
    db_session: Session,
    _mock_celery_module: MagicMock,
) -> None:
    user = _create_user(db_session, plan_tier="pro")
    service = BacktestService(db_session)
    payload = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("40"), "period": 14}],
    )

    run = service.create_and_dispatch(user, payload)

    _assert_sent_outbox(db_session, run.id, task_name="backtests.run", model_type=BacktestRun)


def test_scan_create_and_dispatch_preserves_pending_outbox_on_send_failure(
    db_session: Session,
    _mock_celery_module: MagicMock,
) -> None:
    from backtestforecast.models import ScannerJob

    user = _create_user(db_session, plan_tier="premium")
    service = ScanService(db_session)
    payload = CreateScannerJobRequest(
        name="Dispatch regression scan",
        mode="basic",
        symbols=["AAPL"],
        strategy_types=["long_call"],
        rule_sets=[{"name": "RSI", "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": Decimal("40"), "period": 14}]}],
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        max_recommendations=5,
    )
    _mock_celery_module.send_task.side_effect = ConnectionError("broker down")

    with patch.object(ScanService, "_count_compatible_candidates", return_value=(1, [])):
        job = service.create_and_dispatch_job(user, payload)

    _assert_pending_outbox(db_session, job.id, task_name="scans.run_job", model_type=ScannerJob)


def test_repair_stranded_jobs_requeues_missing_dispatch_state(
    db_session: Session,
    _mock_celery_module: MagicMock,
) -> None:
    user = _create_user(db_session, plan_tier="premium")
    job = ScannerJob(
        user_id=user.id,
        name="Stranded scan",
        status="queued",
        mode="basic",
        plan_tier_snapshot=user.plan_tier,
        candidate_count=1,
        request_snapshot_json={},
        request_hash="abc123",
        created_at=datetime.now(UTC) - timedelta(minutes=20),
        ranking_version="scanner-ranking-v1",
        engine_version="options-multileg-v2",
    )
    db_session.add(job)
    db_session.commit()

    counts = repair_stranded_jobs(
        db_session,
        logger=MagicMock(),
        action="requeue",
        older_than=timedelta(minutes=5),
    )

    assert counts["found"] == 1
    assert counts["requeued"] == 1
    _assert_sent_outbox(db_session, job.id, task_name="scans.run_job", model_type=ScannerJob)


def test_sweep_create_and_dispatch_preserves_pending_outbox_on_send_failure(
    db_session: Session,
    _mock_celery_module: MagicMock,
) -> None:
    from backtestforecast.models import SweepJob

    user = _create_user(db_session, plan_tier="premium")
    service = SweepService(db_session)
    payload = CreateSweepRequest(
        symbol="SPY",
        strategy_types=["bull_put_credit_spread"],
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        entry_rule_sets=[{"name": "no_filter", "entry_rules": []}],
    )
    _mock_celery_module.send_task.side_effect = ConnectionError("broker down")

    with patch.object(SweepService, "_compute_candidate_count", return_value=1):
        job = service.create_and_dispatch_job(user, payload)

    _assert_pending_outbox(db_session, job.id, task_name="sweeps.run", model_type=SweepJob)


def test_analysis_create_and_dispatch_preserves_pending_outbox_on_send_failure(
    db_session: Session,
    _mock_celery_module: MagicMock,
) -> None:
    from backtestforecast.models import SymbolAnalysis

    user = _create_user(db_session, plan_tier="premium")
    service = SymbolDeepAnalysisService(db_session, market_data_fetcher=None, backtest_executor=None)
    payload = CreateAnalysisRequest(symbol="AAPL", idempotency_key="analysis-regression-key")
    _mock_celery_module.send_task.side_effect = ConnectionError("broker down")

    analysis = service.create_and_dispatch_analysis(
        user,
        payload.symbol,
        idempotency_key=payload.idempotency_key,
    )

    _assert_pending_outbox(
        db_session,
        analysis.id,
        task_name="analysis.deep_symbol",
        model_type=SymbolAnalysis,
    )


def test_export_create_and_dispatch_preserves_pending_outbox_on_send_failure(
    db_session: Session,
    _mock_celery_module: MagicMock,
) -> None:
    from backtestforecast.models import ExportJob

    user = _create_user(db_session, plan_tier="pro")
    run = _create_succeeded_backtest(db_session, user.id)
    service = ExportService(db_session)
    payload = CreateExportRequest(run_id=run.id, format=ExportFormat.CSV)
    _mock_celery_module.send_task.side_effect = ConnectionError("broker down")

    export_job = service.create_and_dispatch_export(
        user,
        payload,
        request_id="req-export-regression",
        ip_address="127.0.0.1",
    )

    _assert_pending_outbox(
        db_session,
        export_job.id,
        task_name="exports.generate",
        model_type=ExportJob,
    )


def test_backtest_idempotency_reuse_redispatches_stale_queued_job(
    db_session: Session,
    _mock_celery_module: MagicMock,
) -> None:
    user = _create_user(db_session, plan_tier="pro")
    service = BacktestService(db_session)
    payload = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("40"), "period": 14}],
        idempotency_key="bt-stale-idem",
    )

    run = service.enqueue(user, payload)
    db_session.commit()
    _mark_job_stale(db_session, run)
    _mock_celery_module.send_task.return_value = MagicMock(id="fresh-task-id")

    reused = service.create_and_dispatch(user, payload)

    assert reused.id == run.id
    _assert_stale_job_redispatched(db_session, run.id, task_name="backtests.run", model_type=BacktestRun)


def test_scan_idempotency_reuse_redispatches_stale_queued_job(
    db_session: Session,
    _mock_celery_module: MagicMock,
) -> None:
    from backtestforecast.models import ScannerJob

    user = _create_user(db_session, plan_tier="premium")
    service = ScanService(db_session)
    payload = CreateScannerJobRequest(
        name="Dispatch regression scan",
        mode="basic",
        symbols=["AAPL"],
        strategy_types=["long_call"],
        rule_sets=[{"name": "RSI", "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": Decimal("40"), "period": 14}]}],
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        max_recommendations=5,
        idempotency_key="scan-stale-idem",
    )

    with patch.object(ScanService, "_count_compatible_candidates", return_value=(1, [])):
        job = service.create_job(user, payload)
        db_session.commit()
        _mark_job_stale(db_session, job)
        _mock_celery_module.send_task.return_value = MagicMock(id="fresh-task-id")
        reused = service.create_and_dispatch_job(user, payload)

    assert reused.id == job.id
    _assert_stale_job_redispatched(db_session, job.id, task_name="scans.run_job", model_type=ScannerJob)


def test_sweep_idempotency_reuse_redispatches_stale_queued_job(
    db_session: Session,
    _mock_celery_module: MagicMock,
) -> None:
    from backtestforecast.models import SweepJob

    user = _create_user(db_session, plan_tier="premium")
    service = SweepService(db_session)
    payload = CreateSweepRequest(
        symbol="SPY",
        strategy_types=["bull_put_credit_spread"],
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        entry_rule_sets=[{"name": "no_filter", "entry_rules": []}],
        idempotency_key="sweep-stale-idem",
    )

    with patch.object(SweepService, "_compute_candidate_count", return_value=1):
        job = service.create_job(user, payload)
        db_session.commit()
        _mark_job_stale(db_session, job)
        _mock_celery_module.send_task.return_value = MagicMock(id="fresh-task-id")
        reused = service.create_and_dispatch_job(user, payload)

    assert reused.id == job.id
    _assert_stale_job_redispatched(db_session, job.id, task_name="sweeps.run", model_type=SweepJob)


def test_analysis_idempotency_reuse_redispatches_stale_queued_job(
    db_session: Session,
    _mock_celery_module: MagicMock,
) -> None:
    from backtestforecast.models import SymbolAnalysis

    user = _create_user(db_session, plan_tier="premium")
    service = SymbolDeepAnalysisService(db_session, market_data_fetcher=None, backtest_executor=None)
    payload = CreateAnalysisRequest(symbol="AAPL", idempotency_key="analysis-stale-idem")

    analysis = service.create_analysis(user, payload.symbol, idempotency_key=payload.idempotency_key)
    db_session.commit()
    _mark_job_stale(db_session, analysis)
    _mock_celery_module.send_task.return_value = MagicMock(id="fresh-task-id")

    reused = service.create_and_dispatch_analysis(user, payload.symbol, idempotency_key=payload.idempotency_key)

    assert reused.id == analysis.id
    _assert_stale_job_redispatched(
        db_session,
        analysis.id,
        task_name="analysis.deep_symbol",
        model_type=SymbolAnalysis,
    )


def test_export_idempotency_reuse_redispatches_stale_queued_job(
    db_session: Session,
    _mock_celery_module: MagicMock,
) -> None:
    from backtestforecast.models import ExportJob

    user = _create_user(db_session, plan_tier="pro")
    run = _create_succeeded_backtest(db_session, user.id)
    service = ExportService(db_session)
    payload = CreateExportRequest(run_id=run.id, format=ExportFormat.CSV, idempotency_key="export-stale-idem")

    export_job = service.enqueue_export(user, payload)
    db_session.commit()
    _mark_job_stale(db_session, export_job)
    _mock_celery_module.send_task.return_value = MagicMock(id="fresh-task-id")

    reused = service.create_and_dispatch_export(user, payload)

    assert reused.id == export_job.id
    _assert_stale_job_redispatched(
        db_session,
        export_job.id,
        task_name="exports.generate",
        model_type=ExportJob,
    )
