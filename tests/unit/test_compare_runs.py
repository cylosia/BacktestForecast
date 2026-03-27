"""Item 78: compare_runs rejects non-terminal runs.

Verify that BacktestService.compare_runs raises ValidationError when a run
has status 'running' (not 'succeeded').
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from backtestforecast.errors import AppValidationError
from backtestforecast.models import BacktestEquityPoint, BacktestRun, BacktestTrade, User
from backtestforecast.schemas.backtests import CompareBacktestsRequest
from backtestforecast.services.backtests import BacktestService

pytestmark = pytest.mark.postgres


@pytest.fixture()
def db_session(postgres_db_session: Session) -> Session:
    return postgres_db_session


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


def _set_user_plan(user: User, tier: str) -> None:
    user.plan_tier = tier
    user.subscription_status = "active"


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


def test_to_detail_response_marks_equity_curve_truncated_when_extra_point_is_present(db_session):
    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    service = BacktestService(db_session)

    equity_points = [
        BacktestEquityPoint(
            run_id=run.id,
            trade_date=date(2024, 1, 1),
            equity=Decimal("10000"),
            cash=Decimal("10000"),
            position_value=Decimal("0"),
            drawdown_pct=Decimal("0"),
        )
        for _ in range(10_001)
    ]
    response = service._to_detail_response(run, trades=[], equity_points=equity_points)

    assert response.equity_curve_truncated is True
    assert len(response.equity_curve) == 10_000
    assert response.equity_curve_points_omitted == 1


def test_to_detail_response_uses_full_run_decided_trade_count_not_truncated_subset(db_session):
    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    run.trade_count = 100
    db_session.commit()
    service = BacktestService(db_session)

    trade_subset = [_create_trade(db_session, run, idx) for idx in range(3)]
    response = service._to_detail_response(
        run,
        trades=trade_subset,
        equity_points=[],
        decided_trades=77,
    )

    assert response.summary.trade_count == 100
    assert response.summary.decided_trades == 77
    assert response.summary_provenance == "persisted_run_aggregates"
    assert response.trade_items_omitted == 0


def test_summary_response_uses_persisted_run_metrics_not_transport_slice(db_session):
    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    run.trade_count = 250
    run.total_net_pnl = Decimal("1234.56")
    run.win_rate = Decimal("55.5")
    db_session.commit()

    service = BacktestService(db_session)
    summary = service._summary_response(run, decided_trades=88)

    assert summary.trade_count == 250
    assert summary.decided_trades == 88
    assert summary.total_net_pnl == Decimal("1234.56")
    assert summary.win_rate == Decimal("55.5")


def test_summary_response_preserves_infinite_ratio_metrics(db_session):
    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    run.profit_factor = Decimal("Infinity")
    run.payoff_ratio = Decimal("-Infinity")
    run.recovery_factor = Decimal("Infinity")

    service = BacktestService(db_session)
    summary = service._summary_response(run, decided_trades=1)

    dumped = summary.model_dump(mode="json")
    assert dumped["profit_factor"] == "Infinity"
    assert dumped["payoff_ratio"] == "-Infinity"
    assert dumped["recovery_factor"] == "Infinity"


@pytest.mark.target_assertion
def test_get_run_for_owner_marks_equity_curve_truncated_when_db_has_extra_point(db_session, target_assertion):
    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    service = BacktestService(db_session)

    for idx in range(10_001):
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
    db_session.expire_all()

    response = service.get_run_for_owner(user_id=user.id, run_id=run.id)

    target_assertion()
    assert response.equity_curve_truncated is True
    assert response.summary_provenance == "persisted_run_aggregates"
    assert response.equity_curve_points_omitted == 1


def test_compare_runs_marks_summary_provenance_from_persisted_aggregates(db_session):
    user = _create_user(db_session)
    _set_user_plan(user, "pro")
    run1 = _create_run(db_session, user, "succeeded")
    run2 = _create_run(db_session, user, "succeeded")

    response = BacktestService(db_session).compare_runs(
        user,
        CompareBacktestsRequest(run_ids=[run1.id, run2.id]),
    )

    assert {item.summary_provenance for item in response.items} == {"persisted_run_aggregates"}


def test_get_run_for_owner_warns_when_summary_trade_count_mismatches_persisted_trades(db_session):
    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    run.trade_count = 99
    db_session.add(run)
    db_session.commit()

    _create_trade(db_session, run, 1)
    db_session.commit()

    response = BacktestService(db_session).get_run_for_owner(user_id=user.id, run_id=run.id)
    warning_codes = {warning.code for warning in response.warnings}

    assert "summary_trade_count_mismatch" in warning_codes


def test_get_run_for_owner_warns_when_trade_slice_is_partial(db_session):
    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    run.trade_count = 3
    db_session.add(run)
    db_session.commit()

    for idx in range(3):
        _create_trade(db_session, run, idx)
    db_session.commit()

    response = BacktestService(db_session).get_run_for_owner(user_id=user.id, run_id=run.id, trade_limit=2)
    warning_codes = {warning.code for warning in response.warnings}

    assert len(response.trades) == 2
    assert response.summary.trade_count == 3
    assert response.trade_items_omitted == 1
    assert "partial_trade_payload" in warning_codes


def test_get_run_for_owner_uses_full_run_decided_trade_count_under_trade_limit(db_session):
    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    service = BacktestService(db_session)

    for idx in range(12):
        trade = _create_trade(db_session, run, idx)
        trade.net_pnl = Decimal("0") if idx < 5 else Decimal("99")
    run.trade_count = 12
    db_session.commit()

    response = service.get_run_for_owner(user_id=user.id, run_id=run.id, trade_limit=5)

    assert len(response.trades) == 5
    assert response.summary.trade_count == 12
    assert response.summary.decided_trades == 7


# ---------------------------------------------------------------------------
# Legacy risk_free_rate serialization
# ---------------------------------------------------------------------------

def test_to_detail_response_uses_snapshot_risk_free_rate_for_legacy_runs(db_session):
    """Legacy runs with NULL column values should serialize the snapshotted request risk_free_rate."""
    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    run.risk_free_rate = None
    run.input_snapshot_json = {"risk_free_rate": 0.0125}
    db_session.commit()
    db_session.refresh(run)

    service = BacktestService(db_session)

    response = service._to_detail_response(run, trades=[], equity_points=[])

    assert response.risk_free_rate == Decimal("0.0125")


def test_get_run_for_owner_uses_snapshot_risk_free_rate_for_legacy_runs(db_session):
    """Repository-backed detail fetches should preserve snapshotted risk_free_rate for legacy rows."""
    user = _create_user(db_session)
    run = _create_run(db_session, user, "succeeded")
    run.risk_free_rate = None
    run.input_snapshot_json = {"risk_free_rate": 0.0175}
    db_session.commit()

    service = BacktestService(db_session)

    response = service.get_run_for_owner(user_id=user.id, run_id=run.id)

    assert response.risk_free_rate == Decimal("0.0175")


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


@pytest.mark.target_assertion
def test_compare_runs_marks_truncated_when_any_run_exceeds_trade_limit(db_session, target_assertion):
    """compare_runs should flag truncation from full pre-truncation totals."""
    user = _create_user(db_session)
    _set_user_plan(user, "premium")
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
    large_run_item = next(item for item in result.items if item.id == runs[0].id)

    target_assertion()
    assert result.trade_limit_per_run == 1600
    assert result.trades_truncated is True
    assert len(large_run_item.trades) == result.trade_limit_per_run
    assert large_run_item.summary.trade_count == 1601
    assert large_run_item.trade_items_omitted == 1
    assert all(len(item.trades) == 10 for item in result.items if item.id != runs[0].id)


@pytest.mark.target_assertion
def test_compare_runs_uses_full_run_decided_trade_count(db_session, target_assertion):
    user = _create_user(db_session)
    _set_user_plan(user, "premium")
    runs = [_create_run(db_session, user, "succeeded") for _ in range(5)]

    for idx in range(1601):
        trade = _create_trade(db_session, runs[0], idx)
        if idx % 2 == 0:
            trade.net_pnl = Decimal("0")
    runs[0].trade_count = 1601
    for run in runs[1:]:
        for idx in range(10):
            _create_trade(db_session, run, idx)
        run.trade_count = 10
    db_session.commit()
    db_session.expire_all()

    service = BacktestService(db_session)
    request = CompareBacktestsRequest(run_ids=[run.id for run in runs])

    result = service.compare_runs(user, request)
    large_run_item = next(item for item in result.items if item.id == runs[0].id)

    target_assertion()
    assert large_run_item.summary.decided_trades == 800


def test_compare_runs_warn_when_summary_trade_count_mismatches_persisted_trade_rows(db_session):
    user = _create_user(db_session)
    _set_user_plan(user, "premium")
    run_a = _create_run(db_session, user, "succeeded")
    run_b = _create_run(db_session, user, "succeeded")
    run_a.trade_count = 50
    db_session.add_all([run_a, run_b])
    db_session.commit()

    _create_trade(db_session, run_a, 1)
    _create_trade(db_session, run_b, 2)
    db_session.commit()

    service = BacktestService(db_session)
    result = service.compare_runs(user, CompareBacktestsRequest(run_ids=[run_a.id, run_b.id]))

    warning_codes = {warning.code for warning in result.items[0].warnings}
    assert "summary_trade_count_mismatch" in warning_codes
