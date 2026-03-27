"""Full-stack smoke test for the nightly pipeline.

Requires PostgreSQL and Redis to be available (skips otherwise).
Validates that:
  1. The pipeline runs end-to-end without error
  2. A NightlyPipelineRun is persisted with status='succeeded'
  3. DailyRecommendations are generated
  4. Re-running for the same trade_date is idempotent (no duplicate)
  5. Schema constraints (check constraints, defaults, indexes) are enforced
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from backtestforecast.market_data.types import DailyBar
from backtestforecast.models import DailyRecommendation, NightlyPipelineRun
from backtestforecast.pipeline.service import NightlyPipelineService

# ---------------------------------------------------------------------------
# Mocks
# ---------------------------------------------------------------------------


class MockMarketDataFetcher:
    """Returns fake daily bar data (400 days) for any symbol."""

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> list[DailyBar]:
        bars: list[DailyBar] = []
        current = start_date
        base_price = 100.0
        while current <= end_date:
            # Simple random-walk-ish prices for regime classification
            idx = (current - start_date).days
            close = base_price + idx * 0.1 + (idx % 10) * 0.5
            high = close + 1.0
            low = close - 1.0
            open_p = close - 0.2
            bars.append(
                DailyBar(
                    trade_date=current,
                    open_price=open_p,
                    high_price=high,
                    low_price=low,
                    close_price=close,
                    volume=1_000_000.0,
                )
            )
            current += timedelta(days=1)
        return bars

    def get_earnings_dates(self, symbol: str, start_date: date, end_date: date) -> set[date]:
        return set()


class MockBacktestExecutor:
    """Returns fake backtest results."""

    def run_quick_backtest(
        self,
        symbol: str,
        strategy_type: str,
        start_date: date,
        end_date: date,
        target_dte: int = 30,
        strategy_overrides: dict | None = None,
    ) -> dict | None:
        return {
            "trade_count": 15,
            "win_rate": 65.0,
            "total_roi_pct": 12.5,
            "total_net_pnl": 1250.0,
            "max_drawdown_pct": 8.0,
        }

    def run_full_backtest(
        self,
        symbol: str,
        strategy_type: str,
        start_date: date,
        end_date: date,
        target_dte: int = 30,
        strategy_overrides: dict | None = None,
    ) -> dict | None:
        return {
            "trade_count": 25,
            "win_rate": 64.0,
            "total_roi_pct": 14.0,
            "total_net_pnl": 1400.0,
            "max_drawdown_pct": 7.0,
            "trades": [
                {"entry_date": "2024-01-15", "exit_date": "2024-02-15", "net_pnl": 50.0, "holding_period_days": 31}
            ],
            "equity_curve": [
                {"trade_date": "2024-01-15", "equity": 10000.0, "drawdown_pct": 0.0},
                {"trade_date": "2024-02-15", "equity": 10150.0, "drawdown_pct": 2.0},
            ],
        }


class MockForecaster:
    """Returns fake forecast data."""

    def get_forecast(
        self,
        symbol: str,
        strategy_type: str,
        horizon_days: int,
        *,
        as_of_date: date | None = None,
    ) -> dict | None:
        return {
            "expected_return_median_pct": 5.0,
            "positive_outcome_rate_pct": 62.0,
            "analog_count": 50,
        }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_pipeline_runs_and_persists(db_session) -> None:
    """Pipeline runs end-to-end, persists run and recommendations."""
    market_data = MockMarketDataFetcher()
    executor = MockBacktestExecutor()
    forecaster = MockForecaster()

    service = NightlyPipelineService(
        db_session,
        market_data_fetcher=market_data,
        backtest_executor=executor,
        forecaster=forecaster,
    )

    trade_date = date.today()
    run = service.run_pipeline(
        trade_date=trade_date,
        symbols=["AAPL", "MSFT"],
        max_full_candidates=10,
        max_recommendations=5,
    )

    assert run.status == "succeeded"
    assert run.recommendations_produced > 0

    recs = db_session.scalars(
        select(DailyRecommendation).where(DailyRecommendation.pipeline_run_id == run.id)
    ).all()
    assert len(recs) == run.recommendations_produced


@pytest.mark.smoke
def test_pipeline_idempotent_on_retry(db_session) -> None:
    """Re-running for the same trade_date returns the same run (no duplicate)."""
    market_data = MockMarketDataFetcher()
    executor = MockBacktestExecutor()
    forecaster = MockForecaster()

    service = NightlyPipelineService(
        db_session,
        market_data_fetcher=market_data,
        backtest_executor=executor,
        forecaster=forecaster,
    )

    trade_date = date.today()
    run1 = service.run_pipeline(
        trade_date=trade_date,
        symbols=["AAPL", "MSFT"],
        max_full_candidates=10,
        max_recommendations=5,
    )
    run2 = service.run_pipeline(
        trade_date=trade_date,
        symbols=["AAPL", "MSFT"],
        max_full_candidates=10,
        max_recommendations=5,
    )

    assert run1.id == run2.id
    assert run2.status == "succeeded"

    succeeded_runs = db_session.scalars(
        select(NightlyPipelineRun).where(
            NightlyPipelineRun.trade_date == trade_date,
            NightlyPipelineRun.status == "succeeded",
        )
    ).all()
    assert len(succeeded_runs) == 1


# ---------------------------------------------------------------------------
# Item 52: pipeline stomper respects age threshold
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_stomper_does_not_fail_recent_run(db_session, smoke_uses_sqlite) -> None:
    """Verify that a pipeline run started 10 minutes ago is NOT marked as
    failed by the stomper. Only truly stale runs should be superseded."""
    if smoke_uses_sqlite:
        pytest.skip(
            "SQLite does not support partial unique indexes; "
            "the trade_date uniqueness prevents two concurrent runs."
        )
    from datetime import UTC, datetime, timedelta

    recent_run = NightlyPipelineRun(
        trade_date=date.today(),
        status="running",
        stage="quick_backtest",
        created_at=datetime.now(UTC) - timedelta(minutes=10),
        started_at=datetime.now(UTC) - timedelta(minutes=10),
    )
    db_session.add(recent_run)
    db_session.commit()
    db_session.refresh(recent_run)

    market_data = MockMarketDataFetcher()
    executor = MockBacktestExecutor()
    forecaster = MockForecaster()

    service = NightlyPipelineService(
        db_session,
        market_data_fetcher=market_data,
        backtest_executor=executor,
        forecaster=forecaster,
    )

    run = service.run_pipeline(
        trade_date=date.today(),
        symbols=["AAPL"],
        max_full_candidates=5,
        max_recommendations=3,
    )

    db_session.expire_all()
    original = db_session.get(NightlyPipelineRun, recent_run.id)
    assert original is not None
    assert run.id == recent_run.id
    assert run.status == "running"
    assert original.status == "running"


@pytest.mark.smoke
def test_schema_constraints_enforced(db_session, smoke_uses_sqlite) -> None:
    """Check constraint rejects invalid status. Skip on SQLite (no CHECK enforcement)."""
    if smoke_uses_sqlite:
        pytest.skip("SQLite does not enforce CHECK constraints by default")

    invalid_run = NightlyPipelineRun(
        trade_date=date.today(),
        status="invalid",
        stage="universe_screen",
    )
    db_session.add(invalid_run)

    with pytest.raises(IntegrityError):
        db_session.commit()

    db_session.rollback()
