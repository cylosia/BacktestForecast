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
from backtestforecast.errors import AppValidationError
from backtestforecast.models import BacktestRun, BacktestTrade, User
from backtestforecast.schemas.backtests import CompareBacktestsRequest
from backtestforecast.services.backtests import BacktestService


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
    user = User(clerk_user_id="compare_test_user", email="compare@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_trade(session: Session, run: BacktestRun, idx: int) -> BacktestTrade:
    trade = BacktestTrade(
        run_id=run.id,
        option_ticker=f"O:TEST{idx}",
        strategy_type=run.strategy_type,
        underlying_symbol=run.symbol,
        entry_date=date(2024, 1, 1),
        exit_date=date(2024, 1, 2),
        expiration_date=date(2024, 2, 1),
        quantity=1,
        dte_at_open=30,
        holding_period_days=1,
        entry_underlying_close=Decimal("100"),
        exit_underlying_close=Decimal("101"),
        entry_mid=Decimal("2"),
        exit_mid=Decimal("3"),
        gross_pnl=Decimal("100"),
        net_pnl=Decimal("99"),
        total_commissions=Decimal("1"),
        entry_reason="entry_rules_met",
        exit_reason="profit_target",
    )
    session.add(trade)
    session.flush()
    return trade


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
    """compare_runs must raise AppValidationError when a run has status 'running'."""
    user = _create_user(db_session)
    succeeded_run = _create_run(db_session, user, "succeeded")
    running_run = _create_run(db_session, user, "running")

    service = BacktestService(db_session)
    request = CompareBacktestsRequest(run_ids=[succeeded_run.id, running_run.id])

    with pytest.raises(AppValidationError, match="succeeded"):
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


def test_compare_runs_marks_truncated_when_any_run_exceeds_trade_limit(db_session):
    """compare_runs should flag truncation from full pre-truncation totals."""
    user = _create_user(db_session)
    runs = [_create_run(db_session, user, "succeeded") for _ in range(5)]

    for idx in range(1601):
        _create_trade(db_session, runs[0], idx)
    runs[0].trade_count = 1601
    for run in runs[1:]:
        for idx in range(10):
            _create_trade(db_session, run, idx)
        run.trade_count = 10
    db_session.commit()

    service = BacktestService(db_session)
    request = CompareBacktestsRequest(run_ids=[run.id for run in runs])

    result = service.compare_runs(user, request)

    assert result.trade_limit_per_run == 1600
    assert result.trades_truncated is True
    assert len(result.items[0].trades) == result.trade_limit_per_run
    assert result.items[0].summary.trade_count == 1601
    assert all(len(item.trades) == 10 for item in result.items[1:])
