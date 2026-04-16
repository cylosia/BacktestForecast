from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import grid_search_weekly_calendar_policy_two_stage as two_stage  # noqa: E402
import run_weekly_calendar_policy_two_stage_batch as batch  # noqa: E402


def test_is_completed_output_requires_matching_regime_mode() -> None:
    output_path = Path("C:/Users/Administrator/BacktestForecast/logs/test_batch_regime_mode.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            {
                "selection_objective": "median",
                "selection_regime_mode": "best_regime_only",
                "combined_best_result": {
                    "trade_count": 42,
                },
            }
        ),
        encoding="utf-8",
    )

    assert batch._is_completed_output(
        output_path,
        objective="median",
        regime_mode="best_regime_only",
    )
    assert not batch._is_completed_output(
        output_path,
        objective="median",
        regime_mode="all",
    )
    output_path.unlink(missing_ok=True)


def test_batch_parse_args_defaults_to_best_regime_only(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["run_weekly_calendar_policy_two_stage_batch.py", "--symbols", "AAPL"])
    args = batch._parse_args()
    assert args.regime_mode == "best_regime_only"


def test_grid_search_parse_args_defaults_to_best_regime_only(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "grid_search_weekly_calendar_policy_two_stage.py",
            "--symbol",
            "AAPL",
            "--start-date",
            "2024-01-01",
        ],
    )
    args = two_stage._parse_args()
    assert args.regime_mode == two_stage.REGIME_MODE_BEST_REGIME_ONLY
