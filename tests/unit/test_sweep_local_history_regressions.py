from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
from backtestforecast.market_data.service import HistoricalDataBundle
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord
from backtestforecast.models import SweepJob, SweepResult, User
from backtestforecast.schemas.sweeps import CreateSweepRequest
from backtestforecast.services.sweeps import SweepService

pytestmark = pytest.mark.postgres


@pytest.fixture()
def db_session(postgres_db_session: Session) -> Session:
    return postgres_db_session


def _create_user(session: Session) -> User:
    user = User(clerk_user_id="sweep-local-history-user", email="sweep-local-history@example.com")
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _create_running_job(session: Session, user: User, payload: CreateSweepRequest) -> SweepJob:
    job = SweepJob(
        user_id=user.id,
        symbol=payload.symbol,
        mode="grid",
        status="running",
        plan_tier_snapshot="pro",
        candidate_count=1,
        request_snapshot_json=payload.model_dump(mode="json"),
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def _summary() -> SimpleNamespace:
    return SimpleNamespace(
        trade_count=12,
        decided_trades=12,
        win_rate=Decimal("58.33"),
        total_roi_pct=Decimal("6.25"),
        average_win_amount=Decimal("120.50"),
        average_loss_amount=Decimal("-90.25"),
        average_holding_period_days=Decimal("5"),
        average_dte_at_open=Decimal("14"),
        max_drawdown_pct=Decimal("4.5"),
        total_commissions=Decimal("15.60"),
        total_net_pnl=Decimal("625.00"),
        starting_equity=Decimal("10000"),
        ending_equity=Decimal("10625"),
        profit_factor=Decimal("1.4"),
        payoff_ratio=Decimal("1.2"),
        expectancy=Decimal("52.08"),
        sharpe_ratio=Decimal("1.1"),
        sortino_ratio=Decimal("1.3"),
        cagr_pct=Decimal("12.5"),
        calmar_ratio=Decimal("0.9"),
        max_consecutive_wins=3,
        max_consecutive_losses=2,
        recovery_factor=Decimal("1.8"),
    )


def test_execute_sweep_persists_results_with_historical_gateway(db_session: Session) -> None:
    payload = CreateSweepRequest(
        symbol="F",
        strategy_types=["long_put"],
        start_date=date(2015, 1, 2),
        end_date=date(2015, 2, 27),
        target_dte=14,
        dte_tolerance_days=5,
        max_holding_days=7,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        entry_rule_sets=[{"name": "default", "entry_rules": []}],
        max_results=1,
    )
    user = _create_user(db_session)
    job = _create_running_job(db_session, user, payload)

    store = MagicMock()
    store.list_option_contracts.return_value = [
        OptionContractRecord("O:F150220P00015000", "put", date(2015, 2, 20), 15.0, 100.0),
    ]
    store.get_option_quote_for_date.return_value = OptionQuoteRecord(
        date(2015, 1, 2), 0.95, 1.05, None,
    )
    bundle = HistoricalDataBundle(
        bars=[DailyBar(date(2015, 1, 2), 15.1, 15.3, 14.9, 15.0, 10_000_000)],
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=HistoricalOptionGateway(store, "F"),
        data_source="historical_flatfile",
        warnings=[],
    )
    result = SimpleNamespace(summary=_summary(), trades=[], equity_curve=[], warnings=[])

    execution_service = MagicMock()
    execution_service.market_data_service.prepare_backtest.return_value = bundle
    execution_service.execute_request.return_value = result

    service = SweepService(db_session, execution_service=execution_service)
    service._execute_sweep(job, payload)
    db_session.commit()
    db_session.expire_all()

    refreshed_job = db_session.get(SweepJob, job.id)
    persisted_results = list(
        db_session.scalars(select(SweepResult).where(SweepResult.sweep_job_id == job.id))
    )

    assert refreshed_job is not None
    assert refreshed_job.status == "succeeded"
    assert refreshed_job.result_count == 1
    assert refreshed_job.prefetch_summary_json is not None
    assert refreshed_job.prefetch_summary_json["dates_processed"] == 1
    assert len(persisted_results) == 1
    assert persisted_results[0].strategy_type == "long_put"
    assert persisted_results[0].summary_json["trade_count"] == 12
