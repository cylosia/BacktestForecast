from __future__ import annotations

import asyncio
from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.billing.entitlements import ExportFormat
from backtestforecast.db.base import Base
from backtestforecast.models import (
    BacktestEquityPoint,
    BacktestRun,
    ScannerJob,
    ScannerRecommendation,
    SweepJob,
    SweepResult,
    User,
)
from backtestforecast.services.backtests import BacktestService
from backtestforecast.services.exports import ExportService
from backtestforecast.services.scans import ScanService
from backtestforecast.services.sweeps import SweepService
from tests.conftest import strip_partial_indexes_for_sqlite as _strip_partial_indexes_for_sqlite

pytestmark = pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")


@pytest.fixture(autouse=True)
def _close_leaked_event_loop() -> None:
    """Clean up stray Windows event loops leaked by earlier tests in the same session."""
    policy = asyncio.get_event_loop_policy()
    local = getattr(policy, "_local", None)
    loop = getattr(local, "_loop", None) if local is not None else None
    if loop is not None and not loop.is_running() and not loop.is_closed():
        loop.close()
    if local is not None and hasattr(local, "_loop"):
        local._loop = None
    yield


@pytest.fixture()
def db_session() -> Session:
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
    user = User(clerk_user_id="execution-params-user", email="execution-params@test.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_run(session: Session, user: User) -> BacktestRun:
    run = BacktestRun(
        user_id=user.id,
        status="succeeded",
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
        trade_count=2,
        input_snapshot_json={},
    )
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def test_detail_returns_null_risk_free_rate_when_legacy_run_lacks_persisted_value(db_session: Session) -> None:
    user = _create_user(db_session)
    run = _create_run(db_session, user)
    run.risk_free_rate = None
    run.input_snapshot_json = {}
    db_session.commit()

    response = BacktestService(db_session)._to_detail_response(run, trades=[], equity_points=[])

    assert response.risk_free_rate is None


def test_export_snapshot_uses_export_limit_not_detail_limit(db_session: Session) -> None:
    user = _create_user(db_session)
    run = _create_run(db_session, user)
    run.risk_free_rate = Decimal("0.031")
    run.input_snapshot_json = {
        "risk_free_rate": 0.031,
        "resolved_risk_free_rate_source": "massive_treasury",
        "resolved_risk_free_rate_field_name": "yield_3_month",
        "dividend_yield": 0.02,
    }
    db_session.commit()

    for idx in range(20_000):
        db_session.add(
                BacktestEquityPoint(
                    run_id=run.id,
                    trade_date=date(2024, 1, 1) + timedelta(days=idx),
                equity=Decimal("10000"),
                cash=Decimal("10000"),
                position_value=Decimal("0"),
                drawdown_pct=Decimal("0"),
            )
        )
    db_session.commit()

    snapshot = ExportService(db_session)._build_export_snapshot(
        user_id=user.id,
        run_id=run.id,
        run_kind="backtest",
        export_format=ExportFormat.CSV,
    )

    assert len(snapshot.equity_curve) == 20_000
    assert snapshot.risk_free_rate == 0.031


def test_scan_recommendation_truncation_uses_persisted_trade_count(db_session: Session) -> None:
    user = _create_user(db_session)
    job = ScannerJob(
        user_id=user.id,
        status="succeeded",
        mode="basic",
        plan_tier_snapshot="pro",
        request_hash="scan-hash",
        request_snapshot_json={},
    )
    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)

    recommendation = ScannerRecommendation(
        scanner_job_id=job.id,
        rank=1,
        score=Decimal("91"),
        symbol="AAPL",
        strategy_type="long_call",
        rule_set_name="default",
        rule_set_hash="abc123",
        request_snapshot_json={},
        summary_json={
            "trade_count": 51,
            "total_commissions": "1",
            "total_net_pnl": "99",
            "starting_equity": "10000",
            "ending_equity": "10099",
        },
        warnings_json=[],
        trades_json=[{
            "option_ticker": "O:ABC",
            "strategy_type": "long_call",
            "underlying_symbol": "AAPL",
            "entry_date": "2024-01-01",
            "exit_date": "2024-01-02",
            "expiration_date": "2024-02-01",
            "quantity": 1,
            "dte_at_open": 30,
            "holding_period_days": 1,
            "entry_underlying_close": "100",
            "exit_underlying_close": "101",
            "entry_mid": "2",
            "exit_mid": "3",
            "gross_pnl": "100",
            "net_pnl": "99",
            "total_commissions": "1",
            "entry_reason": "entry",
            "exit_reason": "exit",
            "detail_json": {},
        }] * 50,
        historical_performance_json={},
        forecast_json={
            "symbol": "AAPL",
            "strategy_type": "long_call",
            "as_of_date": "2024-01-01",
            "horizon_days": 10,
            "analog_count": 5,
            "expected_return_low_pct": "-5",
            "expected_return_median_pct": "2",
            "expected_return_high_pct": "8",
            "summary": "forecast",
            "disclaimer": "test",
        },
        ranking_features_json={
            "current_performance_score": "1",
            "historical_performance_score": "1",
            "forecast_alignment_score": "1",
            "final_score": "1",
        },
        equity_curve_json=[],
    )
    db_session.add(recommendation)
    db_session.commit()
    db_session.refresh(recommendation)

    response = ScanService._to_recommendation_response(recommendation)

    assert response.trades_truncated is True
    assert response.trade_items_omitted == 1
    assert response.equity_curve_points_omitted == 0


def test_sweep_result_truncation_uses_persisted_trade_count(db_session: Session) -> None:
    user = _create_user(db_session)
    job = SweepJob(
        user_id=user.id,
        symbol="SPY",
        mode="grid",
        status="succeeded",
        plan_tier_snapshot="pro",
        candidate_count=1,
        request_snapshot_json={},
    )
    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)

    result = SweepResult(
        sweep_job_id=job.id,
        rank=1,
        score=Decimal("88"),
        strategy_type="long_call",
        parameter_snapshot_json={},
        summary_json={
            "trade_count": 51,
            "total_commissions": "1",
            "total_net_pnl": "99",
            "starting_equity": "10000",
            "ending_equity": "10099",
        },
        trades_json=[{
            "option_ticker": "O:ABC",
            "strategy_type": "long_call",
            "underlying_symbol": "SPY",
            "entry_date": "2024-01-01",
            "exit_date": "2024-01-02",
            "expiration_date": "2024-02-01",
            "quantity": 1,
            "dte_at_open": 30,
            "holding_period_days": 1,
            "entry_underlying_close": "100",
            "exit_underlying_close": "101",
            "entry_mid": "2",
            "exit_mid": "3",
            "gross_pnl": "100",
            "net_pnl": "99",
            "total_commissions": "1",
            "entry_reason": "entry",
            "exit_reason": "exit",
            "detail_json": {},
        }] * 50,
        warnings_json=[],
        equity_curve_json=[],
    )
    db_session.add(result)
    db_session.commit()
    db_session.refresh(result)

    response = SweepService._to_result_response(result)

    assert response.trades_truncated is True
    assert response.trade_items_omitted == 1
    assert response.equity_curve_points_omitted == 0
