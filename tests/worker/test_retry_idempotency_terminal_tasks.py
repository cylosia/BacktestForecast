from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from backtestforecast.models import BacktestRun, ExportJob, ScannerJob, SweepJob, User

pytestmark = pytest.mark.postgres


@pytest.fixture()
def db_session(postgres_db_session: Session) -> Session:
    return postgres_db_session


def _create_user(session: Session) -> User:
    user = User(clerk_user_id="retry-idempotency-user", email="retry-idempotency@example.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_backtest_run(session: Session, user: User) -> BacktestRun:
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
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


@pytest.mark.parametrize("model_name", ["backtest", "export", "scan", "sweep"])
def test_validate_task_ownership_rejects_redelivery_for_succeeded_jobs(db_session: Session, model_name: str) -> None:
    import apps.worker.app.tasks as tasks_module

    user = _create_user(db_session)
    run = _create_backtest_run(db_session, user)

    if model_name == "backtest":
        job = run
        model_cls = BacktestRun
    elif model_name == "export":
        job = ExportJob(
            user_id=user.id,
            backtest_run_id=run.id,
            export_format="csv",
            status="succeeded",
            file_name="done.csv",
            mime_type="text/csv",
            size_bytes=128,
            sha256_hex="abc123",
            storage_key="exports/done.csv",
            completed_at=datetime.now(UTC),
            celery_task_id="old-task-id",
        )
        model_cls = ExportJob
    elif model_name == "scan":
        job = ScannerJob(
            user_id=user.id,
            status="succeeded",
            mode="basic",
            plan_tier_snapshot="pro",
            request_hash="scan-hash",
            request_snapshot_json={},
            recommendation_count=1,
            completed_at=datetime.now(UTC),
            celery_task_id="old-task-id",
        )
        model_cls = ScannerJob
    else:
        job = SweepJob(
            user_id=user.id,
            symbol="SPY",
            mode="grid",
            status="succeeded",
            plan_tier_snapshot="pro",
            candidate_count=1,
            result_count=1,
            request_snapshot_json={},
            completed_at=datetime.now(UTC),
            celery_task_id="old-task-id",
        )
        model_cls = SweepJob

    if model_name != "backtest":
        db_session.add(job)
        db_session.commit()
        db_session.refresh(job)

    claimed = tasks_module._validate_task_ownership(
        db_session, model_cls, job.id, "new-task-id"
    )

    assert claimed is False
    db_session.expire_all()
    refreshed = db_session.get(model_cls, job.id)
    assert refreshed.celery_task_id == "old-task-id"
