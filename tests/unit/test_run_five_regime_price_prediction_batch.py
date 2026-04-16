from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import run_five_regime_price_prediction_batch as module  # noqa: E402


def test_load_symbols_discovers_when_no_explicit_inputs(monkeypatch) -> None:
    monkeypatch.setattr(module.evaluator, "_discover_symbols", lambda **_: ["AAPL", "MSFT"])

    args = SimpleNamespace(
        symbols=None,
        symbols_file=None,
        min_start_date=module.evaluator.DEFAULT_MIN_START_DATE,
        requested_end_date=module.evaluator.DEFAULT_REQUESTED_END_DATE,
    )

    assert module._load_symbols(args) == ["AAPL", "MSFT"]


def test_summary_row_from_payload_flattens_best_result() -> None:
    payload = {
        "objective": "balanced_accuracy",
        "forward_weeks": 1,
        "threshold_configs": [
            {"label": "n0.75_h2.5", "neutral_move_pct": 0.75, "heavy_move_pct": 2.5},
            {"label": "n1_h3", "neutral_move_pct": 1.0, "heavy_move_pct": 3.0},
        ],
        "feature_gate_configs": [
            {"label": "emanone_hvolnone", "ema_gap_threshold_pct": None, "heavy_vol_threshold_pct": None},
            {"label": "ema0.5_hvol25", "ema_gap_threshold_pct": 0.5, "heavy_vol_threshold_pct": 25.0},
        ],
        "symbols": [
            {
                "symbol": "AGQ",
                "start_date": "2015-01-02",
                "latest_available_date": "2026-04-02",
                "requested_end_date": "2026-04-02",
                "observation_count": 500,
                "scored_config_count": 20736,
                "constraint_passing_config_count": 128,
                "best_result_selection": "constraint_passed",
                "best_result": {
                    "indicator_periods": "roc42_adx7_rsi7",
                    "roc_period": 42,
                    "adx_period": 7,
                    "rsi_period": 7,
                    "bull_filter": "roc0_adx10_rsinone",
                    "bear_filter": "roc0_adx14_rsinone",
                    "threshold_config": "n1_h3",
                    "neutral_move_pct": 1.0,
                    "heavy_move_pct": 3.0,
                    "feature_gate": "ema0.5_hvol25",
                    "ema_gap_threshold_pct": 0.5,
                    "heavy_vol_threshold_pct": 25.0,
                    "constraint_passed": True,
                    "constraint_fail_reasons": [],
                    "exact_accuracy_pct": 27.2,
                    "directional_accuracy_pct": 41.8,
                    "balanced_accuracy_pct": 33.3,
                    "macro_f1_pct": 22.1,
                    "macro_precision_pct": 28.4,
                    "macro_recall_pct": 33.3,
                    "exact_hit_count": 136,
                    "directional_hit_count": 209,
                },
            }
        ],
    }

    row = module._summary_row_from_payload(
        payload=payload,
        output_path=module.ROOT / "logs" / "batch" / "five_regime_price_predictions" / "x" / "results" / "agq.json",
        log_path=module.ROOT / "logs" / "batch" / "five_regime_price_predictions" / "x" / "logs" / "agq.log",
        elapsed_seconds=12.345,
        status="completed",
    )

    assert row["symbol"] == "AGQ"
    assert row["status"] == "completed"
    assert row["objective"] == "balanced_accuracy"
    assert row["indicator_periods"] == "roc42_adx7_rsi7"
    assert row["bull_filter"] == "roc0_adx10_rsinone"
    assert row["bear_filter"] == "roc0_adx14_rsinone"
    assert row["threshold_config_count"] == 2
    assert row["feature_gate_count"] == 2
    assert row["best_result_selection"] == "constraint_passed"
    assert row["threshold_config"] == "n1_h3"
    assert row["feature_gate"] == "ema0.5_hvol25"
    assert row["constraint_passed"] is True
    assert row["exact_accuracy_pct"] == 27.2
    assert row["directional_hit_count"] == 209
