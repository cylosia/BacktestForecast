from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from backtestforecast.backtests.types import BacktestExecutionResult, BacktestSummary
from backtestforecast.scans.ranking import (
    HistoricalObservation,
    aggregate_historical_performance,
    build_ranking_breakdown,
)
from backtestforecast.schemas.scans import HistoricalAnalogForecastResponse


def test_historical_aggregation_is_recency_weighted() -> None:
    now = datetime.now(UTC)
    aggregate = aggregate_historical_performance(
        [
            HistoricalObservation(
                completed_at=now - timedelta(days=20),
                win_rate=70.0,
                total_roi_pct=18.0,
                total_net_pnl=1200.0,
                max_drawdown_pct=8.0,
            ),
            HistoricalObservation(
                completed_at=now - timedelta(days=300),
                win_rate=40.0,
                total_roi_pct=-10.0,
                total_net_pnl=-500.0,
                max_drawdown_pct=22.0,
            ),
        ],
        reference_time=now,
    )

    assert aggregate.sample_count == 2
    assert float(aggregate.weighted_total_roi_pct) > 0
    assert float(aggregate.weighted_win_rate) > 50


def test_ranking_breakdown_favors_bullish_alignment_when_forecast_is_positive() -> None:
    execution_result = BacktestExecutionResult(
        summary=BacktestSummary(
            trade_count=4,
            decided_trades=10,
            win_rate=75.0,
            total_roi_pct=12.0,
            average_win_amount=200.0,
            average_loss_amount=-80.0,
            average_holding_period_days=8.0,
            average_dte_at_open=30.0,
            max_drawdown_pct=9.0,
            total_commissions=20.0,
            total_net_pnl=900.0,
            starting_equity=10000.0,
            ending_equity=10900.0,
        ),
        trades=[],
        equity_curve=[],
        warnings=[],
    )
    forecast = HistoricalAnalogForecastResponse(
        symbol="AAPL",
        strategy_type="long_call",
        as_of_date=date(2025, 6, 1),
        horizon_days=20,
        analog_count=20,
        expected_return_low_pct="1.5",
        expected_return_median_pct="3.2",
        expected_return_high_pct="6.0",
        positive_outcome_rate_pct="68.0",
        summary="Positive analog drift.",
        disclaimer="Probabilistic only.",
        analog_dates=[],
    )
    historical = aggregate_historical_performance([], reference_time=datetime.now(UTC))

    ranking = build_ranking_breakdown(
        execution_result=execution_result,
        historical_performance=historical,
        forecast=forecast,
        strategy_type="long_call",
        account_size=10000.0,
    )

    assert float(ranking.forecast_alignment_score) > 0
    assert float(ranking.final_score) > 0
