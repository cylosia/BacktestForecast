"""Tests for the scanner ranking algorithm (fixes 60-62).

Covers build_ranking_breakdown, aggregate_historical_performance,
and _forecast_alignment_score via the public interface.
"""
from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

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
    decided_trades: int | None = None,
    win_rate: float = 60.0,
    total_roi_pct: float = 10.0,
    total_net_pnl: float = 2000.0,
    max_drawdown_pct: float = 8.0,
):
    """Return a minimal BacktestExecutionResult-like object with a .summary."""
    from types import SimpleNamespace

    summary = SimpleNamespace(
        trade_count=trade_count,
        decided_trades=trade_count if decided_trades is None else decided_trades,
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
        symbol="TSLA",
        strategy_type="bull_call_debit_spread",
        as_of_date=date(2025, 6, 1),
        analog_count=analog_count,
        expected_return_median_pct=Decimal(str(median_return)),
        expected_return_low_pct=Decimal(str(low_return)),
        expected_return_high_pct=Decimal(str(high_return)),
        positive_outcome_rate_pct=Decimal(str(positive_rate)),
        horizon_days=30,
        summary="Historical analog forecast.",
        disclaimer="Research only.",
    )


def _fake_hist_perf(
    sample_count: int = 6,
    win_rate: float = 58.0,
    roi: float = 8.0,
    drawdown: float = 10.0,
) -> HistoricalPerformanceResponse:
    return HistoricalPerformanceResponse(
        sample_count=sample_count,
        weighted_win_rate=Decimal(str(win_rate)),
        weighted_total_roi_pct=Decimal(str(roi)),
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
        assert float(result.effective_sample_size) < 2.0
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
        """Shorter half-life should weight the recent observation more heavily,
        producing a weighted_win_rate closer to the recent observation's 90%."""
        ref = datetime(2025, 6, 1, tzinfo=UTC)
        obs_recent = HistoricalObservation(
            completed_at=ref - timedelta(days=30),
            win_rate=90.0,
            total_roi_pct=25.0,
            total_net_pnl=5000.0,
            max_drawdown_pct=3.0,
        )
        obs_old = HistoricalObservation(
            completed_at=ref - timedelta(days=365),
            win_rate=50.0,
            total_roi_pct=5.0,
            total_net_pnl=500.0,
            max_drawdown_pct=15.0,
        )
        observations = [obs_recent, obs_old]

        result_long = aggregate_historical_performance(
            observations, reference_time=ref, recency_half_life_days=365
        )
        result_short = aggregate_historical_performance(
            observations, reference_time=ref, recency_half_life_days=30
        )

        long_wr = float(result_long.weighted_win_rate)
        short_wr = float(result_short.weighted_win_rate)

        assert short_wr > long_wr, (
            f"Shorter half-life should weight the recent high-win-rate observation more: "
            f"short_hl={short_wr:.2f}, long_hl={long_wr:.2f}"
        )
        assert short_wr > 70.0, f"Short half-life should pull win rate toward 90%: got {short_wr:.2f}"
        assert long_wr < short_wr, "Long half-life gives more equal weighting"

    def test_non_finite_observations_are_skipped_instead_of_zeroing_aggregate(self):
        ref = datetime(2025, 6, 1, tzinfo=UTC)
        valid = HistoricalObservation(
            completed_at=ref - timedelta(days=2),
            win_rate=70.0,
            total_roi_pct=14.0,
            total_net_pnl=1200.0,
            max_drawdown_pct=6.0,
        )
        malformed = HistoricalObservation(
            completed_at=ref - timedelta(days=1),
            win_rate=float("nan"),
            total_roi_pct=float("inf"),
            total_net_pnl=500.0,
            max_drawdown_pct=5.0,
        )

        result = aggregate_historical_performance([valid, malformed], reference_time=ref)

        assert result.sample_count == 1
        assert float(result.weighted_win_rate) == 70.0
        assert float(result.weighted_total_roi_pct) == 14.0
        assert float(result.weighted_max_drawdown_pct) == 6.0


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
                sample_count=3, win_rate=30.0, roi=-15.0, drawdown=35.0,
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
            forecast=_fake_forecast(median_return=-6.0, positive_rate=65.0),
            strategy_type="bear_put_debit_spread",
            account_size=50000.0,
        )
        assert float(result.forecast_alignment_score) > 0

    def test_bearish_strategy_uses_strategy_aware_positive_outcome_rate_directly(self):
        low_favorable = build_ranking_breakdown(
            execution_result=_fake_execution_result(),
            historical_performance=_fake_hist_perf(),
            forecast=_fake_forecast(median_return=-6.0, positive_rate=40.0),
            strategy_type="bear_put_debit_spread",
            account_size=50_000.0,
        )
        high_favorable = build_ranking_breakdown(
            execution_result=_fake_execution_result(),
            historical_performance=_fake_hist_perf(),
            forecast=_fake_forecast(median_return=-6.0, positive_rate=80.0),
            strategy_type="bear_put_debit_spread",
            account_size=50_000.0,
        )

        assert float(high_favorable.forecast_alignment_score) > float(low_favorable.forecast_alignment_score)

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

    def test_neutral_strategy_ignores_directional_positive_probability(self):
        low_positive = build_ranking_breakdown(
            execution_result=_fake_execution_result(),
            historical_performance=_fake_hist_perf(),
            forecast=_fake_forecast(
                median_return=0.5,
                low_return=-2.0,
                high_return=3.0,
                positive_rate=40.0,
            ),
            strategy_type="iron_condor",
            account_size=50000.0,
        )
        high_positive = build_ranking_breakdown(
            execution_result=_fake_execution_result(),
            historical_performance=_fake_hist_perf(),
            forecast=_fake_forecast(
                median_return=0.5,
                low_return=-2.0,
                high_return=3.0,
                positive_rate=80.0,
            ),
            strategy_type="iron_condor",
            account_size=50000.0,
        )
        assert float(low_positive.forecast_alignment_score) == float(high_positive.forecast_alignment_score)

    def test_reasoning_includes_entries(self):
        result = build_ranking_breakdown(
            execution_result=_fake_execution_result(trade_count=5, max_drawdown_pct=10.0),
            historical_performance=_fake_hist_perf(sample_count=8),
            forecast=_fake_forecast(),
            strategy_type="long_call",
            account_size=50000.0,
        )
        assert len(result.reasoning) >= 2

    def test_reasoning_uses_decided_trade_count_for_multiple_trade_claim(self):
        result = build_ranking_breakdown(
            execution_result=_fake_execution_result(
                trade_count=8,
                decided_trades=2,
                max_drawdown_pct=10.0,
            ),
            historical_performance=_fake_hist_perf(sample_count=0),
            forecast=_fake_forecast(analog_count=0, positive_rate=0),
            strategy_type="long_call",
            account_size=50000.0,
        )

        assert not any("multiple decided trades" in reason for reason in result.reasoning)

    def test_historical_confidence_uses_effective_sample_size_not_raw_sample_count(self):
        execution_result = _fake_execution_result()
        forecast = _fake_forecast(analog_count=0, positive_rate=0)
        stale_history = HistoricalPerformanceResponse(
            sample_count=12,
            effective_sample_size=Decimal("1.5"),
            weighted_win_rate=Decimal("58"),
            weighted_total_roi_pct=Decimal("8"),
            weighted_max_drawdown_pct=Decimal("10"),
            recency_half_life_days=180,
        )
        fresh_history = HistoricalPerformanceResponse(
            sample_count=12,
            effective_sample_size=Decimal("12"),
            weighted_win_rate=Decimal("58"),
            weighted_total_roi_pct=Decimal("8"),
            weighted_max_drawdown_pct=Decimal("10"),
            recency_half_life_days=180,
        )

        stale = build_ranking_breakdown(
            execution_result=execution_result,
            historical_performance=stale_history,
            forecast=forecast,
            strategy_type="long_call",
            account_size=50_000.0,
        )
        fresh = build_ranking_breakdown(
            execution_result=execution_result,
            historical_performance=fresh_history,
            forecast=forecast,
            strategy_type="long_call",
            account_size=50_000.0,
        )

        assert float(stale.historical_performance_score) < float(fresh.historical_performance_score)

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

    def test_current_score_ignores_duplicate_net_pnl_signal(self):
        low_net_pnl = build_ranking_breakdown(
            execution_result=_fake_execution_result(
                trade_count=6,
                win_rate=60.0,
                total_roi_pct=10.0,
                total_net_pnl=500.0,
                max_drawdown_pct=8.0,
            ),
            historical_performance=HistoricalPerformanceResponse(
                sample_count=0, recency_half_life_days=180,
            ),
            forecast=_fake_forecast(analog_count=0, positive_rate=0),
            strategy_type="long_call",
            account_size=50000.0,
        )
        high_net_pnl = build_ranking_breakdown(
            execution_result=_fake_execution_result(
                trade_count=6,
                win_rate=60.0,
                total_roi_pct=10.0,
                total_net_pnl=5000.0,
                max_drawdown_pct=8.0,
            ),
            historical_performance=HistoricalPerformanceResponse(
                sample_count=0, recency_half_life_days=180,
            ),
            forecast=_fake_forecast(analog_count=0, positive_rate=0),
            strategy_type="long_call",
            account_size=50000.0,
        )
        assert float(low_net_pnl.current_performance_score) == float(high_net_pnl.current_performance_score)

    def test_current_score_uses_decided_trades_not_total_trade_count_for_sample_bonus(self):
        lightly_decided = build_ranking_breakdown(
            execution_result=_fake_execution_result(
                trade_count=20,
                decided_trades=2,
                win_rate=100.0,
                total_roi_pct=10.0,
                total_net_pnl=1000.0,
                max_drawdown_pct=8.0,
            ),
            historical_performance=HistoricalPerformanceResponse(
                sample_count=0, recency_half_life_days=180,
            ),
            forecast=_fake_forecast(analog_count=0, positive_rate=0),
            strategy_type="long_call",
            account_size=50000.0,
        )
        fully_decided = build_ranking_breakdown(
            execution_result=_fake_execution_result(
                trade_count=20,
                decided_trades=20,
                win_rate=100.0,
                total_roi_pct=10.0,
                total_net_pnl=1000.0,
                max_drawdown_pct=8.0,
            ),
            historical_performance=HistoricalPerformanceResponse(
                sample_count=0, recency_half_life_days=180,
            ),
            forecast=_fake_forecast(analog_count=0, positive_rate=0),
            strategy_type="long_call",
            account_size=50000.0,
        )
        assert float(lightly_decided.current_performance_score) < float(fully_decided.current_performance_score)


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
