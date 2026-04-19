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


def test_bull_confidence_score_increases_as_indicators_move_further_past_thresholds() -> None:
    bull_filter = module.two_stage.FilterConfig(roc_threshold=0.0, adx_threshold=14.0, rsi_threshold=55.0)

    lower_score = module._bull_confidence_score(
        filter_config=bull_filter,
        indicator_triplet=(5.5, 18.5, 56.0),
    )
    higher_score = module._bull_confidence_score(
        filter_config=bull_filter,
        indicator_triplet=(12.0, 26.0, 66.0),
    )

    assert lower_score > 0.0
    assert higher_score > lower_score


def test_classify_regime_with_confidence_respects_feature_gates() -> None:
    bull_filter = module.two_stage.FilterConfig(roc_threshold=0.0, adx_threshold=14.0, rsi_threshold=55.0)
    bear_filter = module.two_stage.NegativeFilterConfig(roc_threshold=0.0, adx_threshold=18.0, rsi_threshold=45.0)
    triplet = (8.0, 22.0, 62.0)

    gated_regime, gated_confidence = module._classify_regime_with_confidence(
        indicator_triplet=triplet,
        bull_filter=bull_filter,
        bear_filter=bear_filter,
        feature_gate=module.FeatureGateConfig(ema_gap_threshold_pct=0.5, heavy_vol_threshold_pct=None),
        context_row={"ema_gap_pct": 0.1, "realized_vol_pct": 40.0},
    )
    open_regime, open_confidence = module._classify_regime_with_confidence(
        indicator_triplet=triplet,
        bull_filter=bull_filter,
        bear_filter=bear_filter,
        feature_gate=module.FeatureGateConfig(ema_gap_threshold_pct=None, heavy_vol_threshold_pct=None),
        context_row={"ema_gap_pct": 0.1, "realized_vol_pct": 40.0},
    )

    assert gated_regime == "neutral"
    assert gated_confidence == 0.0
    assert open_regime == "heavy_bullish"
    assert open_confidence > 0.0


def test_metric_ranking_key_precision_first_prefers_exactness_after_precision() -> None:
    broader_metrics = {
        "macro_precision_pct": 50.0,
        "exact_accuracy_pct": 25.0,
        "directional_accuracy_pct": 65.0,
        "balanced_accuracy_pct": 45.0,
        "macro_f1_pct": 40.0,
        "observation_count": 100,
    }
    tighter_metrics = {
        "macro_precision_pct": 50.0,
        "exact_accuracy_pct": 30.0,
        "directional_accuracy_pct": 60.0,
        "balanced_accuracy_pct": 40.0,
        "macro_f1_pct": 35.0,
        "observation_count": 100,
    }

    assert module._metric_ranking_key(tighter_metrics, objective="precision_first") > module._metric_ranking_key(
        broader_metrics,
        objective="precision_first",
    )


def test_parse_args_defaults_to_allowing_non_monotonic_forward_returns(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["evaluate_five_regime_price_predictions.py", "--symbols", "AGQ"])

    args = module._parse_args()

    assert args.require_monotonic_forward_returns is False


def test_parse_args_accepts_precision_first_objective(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "evaluate_five_regime_price_predictions.py",
            "--symbols",
            "AGQ",
            "--objective",
            "precision_first",
        ],
    )

    args = module._parse_args()

    assert args.objective == "precision_first"


def test_parse_args_can_require_monotonic_forward_returns(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "evaluate_five_regime_price_predictions.py",
            "--symbols",
            "AGQ",
            "--require-monotonic-forward-returns",
        ],
    )

    args = module._parse_args()

    assert args.require_monotonic_forward_returns is True
