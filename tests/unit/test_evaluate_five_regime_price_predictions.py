from __future__ import annotations

import math
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import evaluate_five_regime_price_predictions as module  # noqa: E402


def test_realized_regime_label_maps_returns_into_five_buckets() -> None:
    assert module._realized_regime_label(forward_return_pct=4.0, neutral_move_pct=1.0, heavy_move_pct=3.0) == "heavy_bullish"
    assert module._realized_regime_label(forward_return_pct=2.0, neutral_move_pct=1.0, heavy_move_pct=3.0) == "bullish"
    assert module._realized_regime_label(forward_return_pct=1.0, neutral_move_pct=1.0, heavy_move_pct=3.0) == "neutral"
    assert module._realized_regime_label(forward_return_pct=-2.0, neutral_move_pct=1.0, heavy_move_pct=3.0) == "bearish"
    assert module._realized_regime_label(forward_return_pct=-4.0, neutral_move_pct=1.0, heavy_move_pct=3.0) == "heavy_bearish"


def test_predicted_regimes_from_masks_prioritize_heavy_and_fill_neutral() -> None:
    predicted = module._predicted_regimes_from_masks(
        observation_count=5,
        bull_mask=(1 << 0) | (1 << 1),
        bear_mask=(1 << 3) | (1 << 4),
        heavy_bull_mask=(1 << 0),
        heavy_bear_mask=(1 << 4),
    )

    assert predicted == [
        "heavy_bullish",
        "bullish",
        "neutral",
        "bearish",
        "heavy_bearish",
    ]


def test_score_predictions_reports_exact_directional_and_balanced_accuracy() -> None:
    metrics = module._score_predictions(
        predicted_regimes=[
            "heavy_bullish",
            "bullish",
            "neutral",
            "bearish",
            "heavy_bearish",
        ],
        actual_regimes=[
            "bullish",
            "bullish",
            "neutral",
            "heavy_bearish",
            "heavy_bearish",
        ],
        forward_returns_pct=[2.5, 1.8, 0.2, -4.5, -3.2],
    )

    assert metrics["observation_count"] == 5
    assert metrics["exact_hit_count"] == 3
    assert metrics["directional_hit_count"] == 5
    assert metrics["exact_accuracy_pct"] == 60.0
    assert metrics["directional_accuracy_pct"] == 100.0
    assert math.isclose(float(metrics["balanced_accuracy_pct"]), 66.6667, rel_tol=0.0, abs_tol=0.0001)
    assert math.isclose(float(metrics["macro_f1_pct"]), 46.6667, rel_tol=0.0, abs_tol=0.0001)
    assert metrics["predicted_counts"] == {
        "heavy_bullish": 1,
        "bullish": 1,
        "neutral": 1,
        "bearish": 1,
        "heavy_bearish": 1,
    }
    assert metrics["actual_counts"] == {
        "heavy_bullish": 0,
        "bullish": 2,
        "neutral": 1,
        "bearish": 0,
        "heavy_bearish": 2,
    }


def test_build_label_threshold_configs_supports_sweeps() -> None:
    args = SimpleNamespace(
        neutral_move_pct=1.0,
        heavy_move_pct=3.0,
        neutral_move_pcts="0.75,1.0",
        heavy_move_pcts="2.5,3.5",
    )

    configs = module._build_label_threshold_configs(args)

    assert [(item.neutral_move_pct, item.heavy_move_pct) for item in configs] == [
        (0.75, 2.5),
        (0.75, 3.5),
        (1.0, 2.5),
        (1.0, 3.5),
    ]


def test_candidate_constraint_summary_flags_count_and_monotonic_failures() -> None:
    metrics = {
        "predicted_counts": {
            "heavy_bullish": 25,
            "bullish": 20,
            "neutral": 5,
            "bearish": 21,
            "heavy_bearish": 22,
        },
        "average_forward_return_by_predicted_regime_pct": {
            "heavy_bearish": -3.0,
            "bearish": -1.0,
            "neutral": 0.5,
            "bullish": 0.2,
            "heavy_bullish": 2.0,
        },
    }

    summary = module._candidate_constraint_summary(
        metrics=metrics,
        min_predicted_regime_count=10,
        require_monotonic_forward_returns=True,
    )

    assert summary["min_predicted_regime_count_passed"] is False
    assert summary["monotonic_forward_return_passed"] is False
    assert summary["constraint_passed"] is False
    assert summary["constraint_fail_reasons"] == [
        "min_predicted_regime_count",
        "monotonic_forward_returns",
    ]
