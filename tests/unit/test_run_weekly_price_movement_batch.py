from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import run_weekly_price_movement_batch as module  # noqa: E402


def test_load_symbols_reads_and_dedupes_symbols_file() -> None:
    symbols_file = module.ROOT / "logs" / "test_weekly_price_movement_symbols.txt"
    symbols_file.parent.mkdir(parents=True, exist_ok=True)
    symbols_file.write_text("aapl\nmsft,AAPL\nqqq\n", encoding="utf-8")

    args = SimpleNamespace(symbols=["spy"], symbols_file=symbols_file)

    assert module._load_symbols(args) == ["SPY", "AAPL", "MSFT", "QQQ"]


def test_summary_row_from_payload_flattens_evaluation_and_latest_prediction() -> None:
    payload = {
        "symbol": "AAPL",
        "selected_method": "mlgb72",
        "selected_method_reason": "best_accuracy_full_window",
        "requested_window": {
            "start_date": "2024-01-01",
            "end_date": "2026-04-17",
        },
        "horizon_bars": 5,
        "loaded_bar_count": 650,
        "window_bar_count": 567,
        "parameters": {
            "prediction_engine": "ml",
            "ml_model_name": "gradient_boosting",
            "requested_prediction_method": "auto",
            "requested_max_analogs": None,
            "warmup_calendar_days": 120,
        },
        "evaluation": {
            "accuracy_pct": 58.4158,
            "balanced_accuracy_pct": 53.0872,
            "directional_accuracy_pct": 58.4158,
            "coverage_pct": 19.9211,
            "observation_count": 101,
            "total_scorable_dates": 507,
            "abstained_count": 406,
            "up_precision_pct": 60.241,
            "down_precision_pct": 50.0,
            "up_recall_pct": 84.7458,
            "down_recall_pct": 21.4286,
        },
        "latest_prediction": {
            "as_of_date": "2026-04-16",
            "prediction_engine": "ml",
            "predicted_direction": "up",
            "predicted_sign": 1,
            "confidence_pct": 76.5,
            "probability_up_pct": 76.5,
            "probability_down_pct": 23.5,
            "predicted_return_median_pct": None,
            "predicted_return_mean_pct": None,
        },
    }

    row = module._summary_row_from_payload(
        payload=payload,
        output_path=module.ROOT / "logs" / "batch" / "weekly_price_movement" / "x" / "results" / "aapl.json",
        log_path=module.ROOT / "logs" / "batch" / "weekly_price_movement" / "x" / "logs" / "aapl.log",
        elapsed_seconds=7.25,
        status="completed",
    )

    assert row["symbol"] == "AAPL"
    assert row["prediction_engine"] == "ml"
    assert row["ml_model_name"] == "gradient_boosting"
    assert row["accuracy_pct"] == 58.4158
    assert row["coverage_pct"] == 19.9211
    assert row["latest_direction"] == "up"
    assert row["latest_confidence_pct"] == 76.5


def test_parse_args_defaults_to_auto_prediction_method(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_weekly_price_movement_batch.py",
            "--symbols",
            "AAPL",
        ],
    )

    args = module._parse_args()

    assert args.prediction_method == module.evaluator.DEFAULT_PREDICTION_METHOD


def test_is_completed_output_checks_requested_window_and_horizon() -> None:
    output_path = module.ROOT / "logs" / "test_weekly_price_movement_batch_aapl.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        """{
  "symbol": "AAPL",
  "target": "sign(close[t+5] / close[t] - 1)",
  "horizon_bars": 5,
  "requested_window": {
    "start_date": "2024-01-01",
    "end_date": "2026-04-17"
  },
  "parameters": {
    "min_candidate_count": 60,
    "min_spacing_bars": 5,
    "requested_prediction_method": "auto",
    "requested_max_analogs": null,
    "warmup_calendar_days": 120
  },
  "latest_prediction": null
}""",
        encoding="utf-8",
    )

    args = SimpleNamespace(
        start_date=module.date(2024, 1, 1),
        end_date=module.date(2026, 4, 17),
        horizon_bars=5,
        min_candidate_count=60,
        min_spacing_bars=5,
        prediction_method="auto",
        max_analogs=None,
        warmup_calendar_days=120,
    )

    assert module._is_completed_output(output_path, args=args) is True


def test_materialize_cache_hit_copies_shared_cache_to_run_output() -> None:
    cache_path = module.ROOT / "logs" / "batch" / "weekly_price_movement" / "_cache" / "results" / "aapl.json"
    run_output_path = module.ROOT / "logs" / "batch" / "weekly_price_movement" / "test_run" / "results" / "aapl.json"
    log_path = module.ROOT / "logs" / "batch" / "weekly_price_movement" / "test_run" / "logs" / "aapl.log"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        """{
  "symbol": "AAPL",
  "target": "sign(close[t+5] / close[t] - 1)",
  "horizon_bars": 5,
  "requested_window": {
    "start_date": "2024-01-01",
    "end_date": "2026-04-17"
  },
  "parameters": {
    "min_candidate_count": 60,
    "min_spacing_bars": 5,
    "requested_prediction_method": "auto",
    "requested_max_analogs": null,
    "warmup_calendar_days": 120,
    "prediction_engine": "analog",
    "ml_model_name": null
  },
  "selected_method": "median20",
  "selected_method_reason": "best_accuracy_full_window",
  "loaded_bar_count": 100,
  "window_bar_count": 80,
  "evaluation": {
    "accuracy_pct": 55.0,
    "balanced_accuracy_pct": 50.0,
    "directional_accuracy_pct": 55.0,
    "coverage_pct": 20.0,
    "observation_count": 50,
    "total_scorable_dates": 250,
    "abstained_count": 200,
    "up_precision_pct": 60.0,
    "down_precision_pct": 40.0,
    "up_recall_pct": 70.0,
    "down_recall_pct": 30.0
  },
  "latest_prediction": null
}""",
        encoding="utf-8",
    )
    args = SimpleNamespace(
        start_date=module.date(2024, 1, 1),
        end_date=module.date(2026, 4, 17),
        horizon_bars=5,
        min_candidate_count=60,
        min_spacing_bars=5,
        prediction_method="auto",
        max_analogs=None,
        warmup_calendar_days=120,
    )
    item = module.SymbolRun(symbol="AAPL", output_path=run_output_path, log_path=log_path, cache_path=cache_path)

    row = module._materialize_cache_hit(
        item=item,
        args=args,
        status="reused_cache",
        source_path=cache_path,
        elapsed_seconds=0.0,
    )

    assert row is not None
    assert row["status"] == "reused_cache"
    assert run_output_path.exists()
    assert log_path.exists()


def test_evaluate_symbol_in_worker_uses_shared_worker_config(monkeypatch) -> None:
    config = module.WorkerConfig(
        database_url="postgresql://example",
        db_statement_timeout_ms=30000,
        start_date=module.date(2024, 1, 1),
        end_date=module.date(2026, 4, 17),
        horizon_bars=5,
        max_analogs=None,
        min_candidate_count=60,
        min_spacing_bars=5,
        warmup_calendar_days=120,
        prediction_method="auto",
    )
    preload_calls: list[dict[str, object]] = []
    evaluate_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        module.evaluator,
        "preload_benchmark_bars",
        lambda **kwargs: preload_calls.append(kwargs) or [],
    )
    monkeypatch.setattr(
        module.evaluator,
        "evaluate_symbol_to_payload",
        lambda **kwargs: evaluate_calls.append(kwargs) or {"symbol": kwargs["symbol"], "evaluation": {}, "parameters": {}},
    )

    module._worker_initialize(config)
    result = module._evaluate_symbol_in_worker("AAPL")

    assert preload_calls
    assert evaluate_calls
    assert result["status"] == "completed"
    assert result["payload"]["symbol"] == "AAPL"
