"""Integration tests: export behavior when DB fails during execution.

Verifies proper rollback and job status update when DB operations fail.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from backtestforecast.models import ExportJob
from backtestforecast.services.exports import ExportService


def test_export_marks_job_failed_and_rolls_back_on_db_failure(
    db_session,
    session_factory,
):
    """When DB fails during export execution, session rolls back and job is marked failed."""
    from datetime import UTC, datetime, timedelta
    from decimal import Decimal

    from backtestforecast.models import BacktestRun, User

    # Create user and succeeded backtest
    user = db_session.query(User).filter(User.clerk_user_id == "clerk_test_user").first()
    if user is None:
        user = User(
            clerk_user_id="clerk_test_user",
            email="test@example.com",
            plan_tier="pro",
            subscription_status="active",
            subscription_current_period_end=datetime.now(UTC) + timedelta(days=30),
        )
        db_session.add(user)
        db_session.commit()
        db_session.refresh(user)

    today = datetime.now(UTC).date()
    start = today - timedelta(days=90)
    run = BacktestRun(
        user_id=user.id,
        status="succeeded",
        symbol="AAPL",
        strategy_type="long_call",
        date_from=start,
        date_to=today - timedelta(days=1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        input_snapshot_json={},
        warnings_json=[],
    )
    db_session.add(run)
    db_session.commit()
    db_session.refresh(run)

    export_job = ExportJob(
        user_id=user.id,
        backtest_run_id=run.id,
        export_format="csv",
        status="queued",
        file_name="test.csv",
        mime_type="text/csv",
        expires_at=datetime.now(UTC) + timedelta(days=30),
    )
    db_session.add(export_job)
    db_session.commit()
    db_session.refresh(export_job)

    export_id = export_job.id

    # Simulate DB failure when fetching backtest detail
    with session_factory() as session:
        service = ExportService(session)
        with patch.object(
            service.backtest_service,
            "get_run_for_owner",
            side_effect=SQLAlchemyError("connection lost"),
        ), pytest.raises(SQLAlchemyError):
            service.execute_export_by_id(export_id)

    # Verify job was marked failed (rollback + re-commit of failure status)
    with session_factory() as session:
        refreshed = session.get(ExportJob, export_id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.error_code == "export_generation_failed"
        assert refreshed.completed_at is not None
