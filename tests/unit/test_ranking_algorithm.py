"""Tests for the scanner ranking algorithm (fixes 60-62).

Covers build_ranking_breakdown, aggregate_historical_performance,
and _forecast_alignment_score via the public interface.
"""
from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from backtestforecast.scans.ranking import (
    HistoricalObservation,
    aggregate_historical_performance,
    build_ranking_breakdown,
    detect_rule_set_bias,
    is_strategy_rule_set_compatible,
    recommendation_sort_key,
)
from backtestforecast.schemas.scans import (
    HistoricalAnalogForecastResponse,
    HistoricalPerformanceResponse,
    RankingBreakdownResponse,
)


def _fake_execution_result(
    trade_count: int = 5,
    win_rate: float = 60.0,
    total_roi_pct: float = 10.0,
    total_net_pnl: float = 2000.0,
    max_drawdown_pct: float = 8.0,
):
    """Return a minimal BacktestExecutionResult-like object with a .summary."""
    from types import SimpleNamespace

    summary = SimpleNamespace(
        trade_count=trade_count,
        win_rate=win_rate,
        total_roi_pct=total_roi_pct,
        total_net_pnl=total_net_pnl,
        max_drawdown_pct=max_drawdown_pct,
    )
    return SimpleNamespace(summary=summary, trades=[], equity_curve=[])


def _fake_forecast(
    analog_count: int = 10,
    median_return: float = 3.0,
    low_return: float = -5.0,
    high_return: float = 12.0,
    positive_rate: float = 62.0,
) -> HistoricalAnalogForecastResponse:
    return HistoricalAnalogForecastResponse(
        analog_count=analog_count,
        expected_return_median_pct=Decimal(str(median_return)),
        expected_return_low_pct=Decimal(str(low_return)),
        expected_return_high_pct=Decimal(str(high_return)),
        positive_outcome_rate_pct=Decimal(str(positive_rate)),
        horizon_days=30,
    )


def _fake_hist_perf(
    sample_count: int = 6,
    win_rate: float = 58.0,
    roi: float = 8.0,
    net_pnl: float = 1500.0,
    drawdown: float = 10.0,
) -> HistoricalPerformanceResponse:
    return HistoricalPerformanceResponse(
        sample_count=sample_count,
        weighted_win_rate=Decimal(str(win_rate)),
        weighted_total_roi_pct=Decimal(str(roi)),
        weighted_total_net_pnl=Decimal(str(net_pnl)),
        weighted_max_drawdown_pct=Decimal(str(drawdown)),
        recency_half_life_days=180,
    )


class TestAggregateHistoricalPerformance:
    def test_empty_observations(self):
        ref = datetime(2025, 6, 1, tzinfo=UTC)
        result = aggregate_historical_performance([], reference_time=ref)
        assert result.sample_count == 0
        assert result.weighted_win_rate is None or result.weighted_win_rate == 0

    def test_single_observation_no_decay(self):
        ref = datetime(2025, 6, 1, tzinfo=UTC)
        obs = HistoricalObservation(
            completed_at=ref,
            win_rate=65.0,
            total_roi_pct=12.0,
            total_net_pnl=2400.0,
            max_drawdown_pct=5.0,
        )
        result = aggregate_historical_performance([obs], reference_time=ref)
        assert result.sample_count == 1
        assert abs(float(result.weighted_win_rate) - 65.0) < 0.01
        assert abs(float(result.weighted_total_roi_pct) - 12.0) < 0.01
        assert abs(float(result.weighted_total_net_pnl) - 2400.0) < 0.01

    def test_recency_weighting(self):
        """More recent observations should carry more weight."""
        ref = datetime(2025, 6, 1, tzinfo=UTC)
        recent = HistoricalObservation(
            completed_at=ref - timedelta(days=1),
            win_rate=80.0,
            total_roi_pct=20.0,
            total_net_pnl=5000.0,
            max_drawdown_pct=3.0,
        )
        old = HistoricalObservation(
            completed_at=ref - timedelta(days=365),
            win_rate=40.0,
            total_roi_pct=-10.0,
            total_net_pnl=-2000.0,
            max_drawdown_pct=30.0,
        )
        result = aggregate_historical_performance([recent, old], reference_time=ref)
        assert result.sample_count == 2
        assert float(result.weighted_win_rate) > 60.0
        assert float(result.weighted_total_roi_pct) > 5.0

    def test_all_observations_same_time(self):
        ref = datetime(2025, 6, 1, tzinfo=UTC)
        obs1 = HistoricalObservation(ref, 60.0, 10.0, 1000.0, 5.0)
        obs2 = HistoricalObservation(ref, 40.0, -5.0, -500.0, 20.0)
        result = aggregate_historical_performance([obs1, obs2], reference_time=ref)
        assert result.sample_count == 2
        assert abs(float(result.weighted_win_rate) - 50.0) < 0.01
        assert abs(float(result.weighted_total_roi_pct) - 2.5) < 0.01

    def test_half_life_parameter(self):
        ref = datetime(2025, 6, 1, tzinfo=UTC)
        obs = HistoricalObservation(
            completed_at=ref - timedelta(days=180),
            win_rate=70.0,
            total_roi_pct=15.0,
            total_net_pnl=3000.0,
            max_drawdown_pct=8.0,
        )
        result_180 = aggregate_historical_performance(
            [obs], reference_time=ref, recency_half_life_days=180
        )
        result_30 = aggregate_historical_performance(
            [obs], reference_time=ref, recency_half_life_days=30
        )
        assert abs(float(result_180.weighted_win_rate) - 70.0) < 0.01
        assert abs(float(result_30.weighted_win_rate) - 70.0) < 0.01


class TestBuildRankingBreakdown:
    def test_positive_scenario_produces_positive_score(self):
        result = build_ranking_breakdown(
            execution_result=_fake_execution_result(
                trade_count=8, win_rate=70.0, total_roi_pct=15.0,
                total_net_pnl=3000.0, max_drawdown_pct=5.0,
            ),
            historical_performance=_fake_hist_perf(),
            forecast=_fake_forecast(median_return=5.0, positive_rate=70.0),
            strategy_type="bull_call_debit_spread",
            account_size=50000.0,
        )
        assert float(result.final_score) > 0

    def test_negative_scenario_produces_negative_score(self):
        result = build_ranking_breakdown(
            execution_result=_fake_execution_result(
                trade_count=2, win_rate=20.0, total_roi_pct=-25.0,
                total_net_pnl=-5000.0, max_drawdown_pct=40.0,
            ),
            historical_performance=_fake_hist_perf(
                sample_count=3, win_rate=30.0, roi=-15.0, net_pnl=-3000.0, drawdown=35.0,
            ),
            forecast=_fake_forecast(median_return=-8.0, positive_rate=30.0),
            strategy_type="bull_call_debit_spread",
            account_size=50000.0,
        )
        assert float(result.final_score) < 0

    def test_zero_historical_samples(self):
        result = build_ranking_breakdown(
            execution_result=_fake_execution_result(),
            historical_performance=HistoricalPerformanceResponse(
                sample_count=0, recency_half_life_days=180,
            ),
            forecast=_fake_forecast(),
            strategy_type="long_call",
            account_size=50000.0,
        )
        assert float(result.historical_performance_score) == 0.0
        assert float(result.final_score) != 0.0

    def test_no_forecast_data(self):
        result = build_ranking_breakdown(
            execution_result=_fake_execution_result(),
            historical_performance=_fake_hist_perf(),
            forecast=_fake_forecast(analog_count=0, positive_rate=0),
            strategy_type="iron_condor",
            account_size=50000.0,
        )
        assert float(result.forecast_alignment_score) == 0.0

    def test_score_weights_sum_to_100(self):
        """The final score should be approximately
        0.55 * current + 0.35 * historical + 0.10 * forecast."""
        result = build_ranking_breakdown(
            execution_result=_fake_execution_result(),
            historical_performance=_fake_hist_perf(),
            forecast=_fake_forecast(),
            strategy_type="long_call",
            account_size=50000.0,
        )
        expected = (
            float(result.current_performance_score) * 0.55
            + float(result.historical_performance_score) * 0.35
            + float(result.forecast_alignment_score) * 0.10
        )
        assert abs(float(result.final_score) - expected) < 0.5

    def test_bearish_strategy_with_bearish_forecast(self):
        result = build_ranking_breakdown(
            execution_result=_fake_execution_result(),
            historical_performance=_fake_hist_perf(),
            forecast=_fake_forecast(median_return=-6.0, positive_rate=35.0),
            strategy_type="bear_put_debit_spread",
            account_size=50000.0,
        )
        assert float(result.forecast_alignment_score) > 0

    def test_neutral_strategy_with_low_dispersion(self):
        result = build_ranking_breakdown(
            execution_result=_fake_execution_result(),
            historical_performance=_fake_hist_perf(),
            forecast=_fake_forecast(
                median_return=0.5, low_return=-2.0, high_return=3.0, positive_rate=52.0
            ),
            strategy_type="iron_condor",
            account_size=50000.0,
        )
        assert float(result.forecast_alignment_score) > 0

    def test_reasoning_includes_entries(self):
        result = build_ranking_breakdown(
            execution_result=_fake_execution_result(trade_count=5, max_drawdown_pct=10.0),
            historical_performance=_fake_hist_perf(sample_count=8),
            forecast=_fake_forecast(),
            strategy_type="long_call",
            account_size=50000.0,
        )
        assert len(result.reasoning) >= 2

    def test_nan_in_execution_result(self):
        result = build_ranking_breakdown(
            execution_result=_fake_execution_result(
                total_roi_pct=float("nan"),
                total_net_pnl=float("inf"),
            ),
            historical_performance=_fake_hist_perf(),
            forecast=_fake_forecast(),
            strategy_type="long_call",
            account_size=50000.0,
        )
        assert math.isfinite(float(result.final_score))


class TestRecommendationSortKey:
    def test_higher_score_sorts_first(self):
        r1 = RankingBreakdownResponse(
            current_performance_score=Decimal("50"),
            historical_performance_score=Decimal("40"),
            forecast_alignment_score=Decimal("30"),
            final_score=Decimal("80"),
            reasoning=[],
        )
        r2 = RankingBreakdownResponse(
            current_performance_score=Decimal("30"),
            historical_performance_score=Decimal("20"),
            forecast_alignment_score=Decimal("10"),
            final_score=Decimal("40"),
            reasoning=[],
        )
        items = [
            ("AAPL", "long_call", "default", r2),
            ("AAPL", "long_call", "default", r1),
        ]
        sorted_items = sorted(items, key=recommendation_sort_key)
        assert float(sorted_items[0][3].final_score) > float(sorted_items[1][3].final_score)


class TestDetectRuleSetBias:
    def test_empty_rules_returns_none(self):
        assert detect_rule_set_bias([]) is None


class TestStrategyRuleSetCompatibility:
    def test_no_rules_always_compatible(self):
        assert is_strategy_rule_set_compatible("long_call", []) is True
