"""Item 78: compare_runs rejects non-terminal runs.

Verify that BacktestService.compare_runs raises ValidationError when a run
has status 'running' (not 'succeeded').
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.errors import ValidationError
from backtestforecast.models import BacktestRun, User
from backtestforecast.schemas.backtests import CompareBacktestsRequest
from backtestforecast.services.backtests import BacktestService


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
    user = User(clerk_user_id="compare_test_user", email="compare@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_run(session: Session, user: User, status: str) -> BacktestRun:
    run = BacktestRun(
        user_id=user.id,
        status=status,
        symbol="AAPL",
        strategy_type="long_call",
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


def test_compare_runs_rejects_running_status(db_session):
    """compare_runs must raise ValidationError when a run has status 'running'."""
    user = _create_user(db_session)
    succeeded_run = _create_run(db_session, user, "succeeded")
    running_run = _create_run(db_session, user, "running")

    service = BacktestService(db_session)
    request = CompareBacktestsRequest(run_ids=[succeeded_run.id, running_run.id])

    with pytest.raises(ValidationError, match="succeeded"):
        service.compare_runs(user, request)


def test_compare_runs_accepts_all_succeeded(db_session):
    """compare_runs should not raise when all runs have status 'succeeded'."""
    user = _create_user(db_session)
    run1 = _create_run(db_session, user, "succeeded")
    run2 = _create_run(db_session, user, "succeeded")

    service = BacktestService(db_session)
    request = CompareBacktestsRequest(run_ids=[run1.id, run2.id])

    result = service.compare_runs(user, request)
    assert len(result.items) == 2


# ---------------------------------------------------------------------------
# Item 53: _to_detail_response receives trades and equity_points params
# ---------------------------------------------------------------------------


def test_to_detail_response_receives_preloaded_data(db_session):
    """Verify _to_detail_response accepts trades and equity_points params,
    avoiding extra queries when data is preloaded."""
    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")

    service = BacktestService(db_session)

    response = service._to_detail_response(
        run,
        trades=[],
        equity_points=[],
    )
    assert response.id == run.id
    assert response.trades == []
    assert response.equity_curve == []


# ---------------------------------------------------------------------------
# Item 68: compare_runs uses get_trades_for_run with limit parameter
# ---------------------------------------------------------------------------


def test_compare_runs_calls_get_trades_with_limit(db_session):
    """Verify compare_runs calls get_trades_for_runs (batch) with an explicit
    limit_per_run instead of eagerly loading trades one-by-one."""
    from unittest.mock import patch

    user = _create_user(db_session)
    run1 = _create_run(db_session, user, "succeeded")
    run2 = _create_run(db_session, user, "succeeded")

    service = BacktestService(db_session)
    request = CompareBacktestsRequest(run_ids=[run1.id, run2.id])

    with patch.object(
        service.run_repository, "get_trades_for_runs", wraps=service.run_repository.get_trades_for_runs
    ) as mock_get_trades:
        service.compare_runs(user, request)

        assert mock_get_trades.call_count == 1
        call_kwargs = mock_get_trades.call_args
        assert "limit_per_run" in call_kwargs.kwargs or len(call_kwargs.args) >= 2, (
            "get_trades_for_runs must be called with an explicit limit_per_run argument"
        )
