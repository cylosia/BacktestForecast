from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from backtestforecast.models import BacktestRun, ExportJob, User
from backtestforecast.repositories.backtest_runs import BacktestRunRepository
from backtestforecast.repositories.export_jobs import ExportJobRepository

pytestmark = pytest.mark.postgres


def _create_user(session) -> User:
    user = User(clerk_user_id="cursor_totals_user", email="cursor@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_run(session, user: User, *, created_at: datetime) -> BacktestRun:
    run = BacktestRun(
        user_id=user.id,
        status="succeeded",
        symbol="AAPL",
        strategy_type="long_call",
        date_from=created_at.date(),
        date_to=created_at.date().replace(day=created_at.day + 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        input_snapshot_json={},
        created_at=created_at,
    )
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def _create_export_job(session, user: User, run: BacktestRun, *, created_at: datetime) -> ExportJob:
    job = ExportJob(
        user_id=user.id,
        backtest_run_id=run.id,
        export_format="csv",
        status="queued",
        file_name="run.csv",
        mime_type="text/csv",
        created_at=created_at,
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def test_backtest_run_cursor_total_ignores_cursor_filter(postgres_db_session: Session):
    user = _create_user(postgres_db_session)
    repo = BacktestRunRepository(postgres_db_session)
    newest = _create_run(postgres_db_session, user, created_at=datetime(2024, 1, 3, tzinfo=UTC))
    middle = _create_run(postgres_db_session, user, created_at=datetime(2024, 1, 2, tzinfo=UTC))
    oldest = _create_run(postgres_db_session, user, created_at=datetime(2024, 1, 1, tzinfo=UTC))

    runs, total = repo.list_for_user_with_count(
        user.id,
        limit=10,
        cursor_before=(middle.created_at, middle.id),
    )

    assert [run.id for run in runs] == [oldest.id]
    assert total == 3
    assert newest.id not in [run.id for run in runs]


def test_export_job_cursor_total_ignores_cursor_filter(postgres_db_session: Session):
    user = _create_user(postgres_db_session)
    run = _create_run(postgres_db_session, user, created_at=datetime(2024, 1, 1, tzinfo=UTC))
    repo = ExportJobRepository(postgres_db_session)
    newest = _create_export_job(postgres_db_session, user, run, created_at=datetime(2024, 1, 3, tzinfo=UTC))
    middle = _create_export_job(postgres_db_session, user, run, created_at=datetime(2024, 1, 2, tzinfo=UTC))
    oldest = _create_export_job(postgres_db_session, user, run, created_at=datetime(2024, 1, 1, tzinfo=UTC))

    jobs, total = repo.list_for_user_with_count(
        user.id,
        limit=10,
        cursor_before=(middle.created_at, middle.id),
    )

    assert [job.id for job in jobs] == [oldest.id]
    assert total == 3
    assert newest.id not in [job.id for job in jobs]
