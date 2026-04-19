from __future__ import annotations

from datetime import date, timedelta
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import benchmark_weekly_regime_models as module  # noqa: E402


def _fridays(count: int) -> list[date]:
    start = date(2026, 1, 2)
    return [start + timedelta(days=7 * index) for index in range(count)]


def test_weekly_analog_prediction_series_uses_only_completed_forward_windows(monkeypatch) -> None:
    fridays = _fridays(5)
    bars = [SimpleNamespace(trade_date=trade_date) for trade_date in fridays]
    bar_index_by_date = {trade_date: index for index, trade_date in enumerate(fridays)}
    evaluation_dates = fridays[:-2]
    forward_returns_pct = [10.0, 20.0, 30.0]
    friday_position_by_date = {trade_date: index for index, trade_date in enumerate(fridays)}

    monkeypatch.setattr(
        module,
        "_build_analog_feature_vectors",
        lambda bars: [(float(index),) for index, _ in enumerate(bars)],
    )
    monkeypatch.setattr(
        module.ANALOG_FORECASTER,
        "_distance",
        lambda left, right: abs(left[0] - right[0]),
    )

    series = module._build_weekly_analog_prediction_series(
        bars=bars,
        bar_index_by_date=bar_index_by_date,
        trading_fridays=fridays,
        evaluation_dates=evaluation_dates,
        forward_returns_pct=forward_returns_pct,
        friday_position_by_date=friday_position_by_date,
        forward_weeks=2,
        neutral_move_pct=1.0,
        heavy_move_pct=3.0,
        max_analogs=5,
        min_candidate_count=1,
    )

    rows = series["rows"]
    assert rows[0]["predicted_regime"] is None
    assert rows[1]["predicted_regime"] is None
    assert rows[2]["candidate_pool_count"] == 1
    assert rows[2]["predicted_return_median_pct"] == 10.0
    assert rows[2]["predicted_regime"] == "heavy_bullish"


def test_aggregate_results_reports_weighted_accuracy_and_improvement_counts() -> None:
    rows = [
        {
            "symbol": "AAA",
            "overlap_observation_count": 10,
            "baseline_overlap_exact_hit_count": 3,
            "baseline_overlap_directional_hit_count": 6,
            "analog_exact_hit_count": 5,
            "analog_directional_hit_count": 7,
            "baseline_overlap_exact_accuracy_pct": 30.0,
            "analog_exact_accuracy_pct": 50.0,
            "baseline_overlap_directional_accuracy_pct": 60.0,
            "analog_directional_accuracy_pct": 70.0,
            "baseline_overlap_macro_precision_pct": 25.0,
            "analog_macro_precision_pct": 35.0,
            "baseline_overlap_macro_f1_pct": 20.0,
            "analog_macro_f1_pct": 30.0,
            "exact_accuracy_delta_pct": 20.0,
            "directional_accuracy_delta_pct": 10.0,
            "macro_precision_delta_pct": 10.0,
            "macro_f1_delta_pct": 10.0,
            "analog_availability_pct": 80.0,
        },
        {
            "symbol": "BBB",
            "overlap_observation_count": 20,
            "baseline_overlap_exact_hit_count": 8,
            "baseline_overlap_directional_hit_count": 12,
            "analog_exact_hit_count": 6,
            "analog_directional_hit_count": 10,
            "baseline_overlap_exact_accuracy_pct": 40.0,
            "analog_exact_accuracy_pct": 30.0,
            "baseline_overlap_directional_accuracy_pct": 60.0,
            "analog_directional_accuracy_pct": 50.0,
            "baseline_overlap_macro_precision_pct": 30.0,
            "analog_macro_precision_pct": 20.0,
            "baseline_overlap_macro_f1_pct": 28.0,
            "analog_macro_f1_pct": 18.0,
            "exact_accuracy_delta_pct": -10.0,
            "directional_accuracy_delta_pct": -10.0,
            "macro_precision_delta_pct": -10.0,
            "macro_f1_delta_pct": -10.0,
            "analog_availability_pct": 90.0,
        },
    ]

    aggregate = module._aggregate_results(rows)

    assert aggregate["symbol_count"] == 2
    assert aggregate["overlap_observation_count"] == 30
    assert aggregate["baseline_weighted_exact_accuracy_pct"] == 36.6667
    assert aggregate["analog_weighted_exact_accuracy_pct"] == 36.6667
    assert aggregate["baseline_weighted_directional_accuracy_pct"] == 60.0
    assert aggregate["analog_weighted_directional_accuracy_pct"] == 56.6667
    assert aggregate["symbols_improved_exact_accuracy"] == 1
    assert aggregate["symbols_improved_macro_precision"] == 1
    assert aggregate["mean_exact_accuracy_delta_pct"] == 5.0
    assert aggregate["median_exact_accuracy_delta_pct"] == 5.0
