from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
import json
from pathlib import Path
import subprocess
import sys
import threading
import time

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
LOGS_DIR = ROOT / "logs"
EVALUATOR_SCRIPT = ROOT / "scripts" / "predict_weekly_price_movement.py"

import predict_weekly_price_movement as evaluator  # noqa: E402


@dataclass(frozen=True, slots=True)
class SymbolRun:
    symbol: str
    output_path: Path
    log_path: Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the weekly price-movement evaluator across multiple symbols with resumable "
            "per-symbol outputs and a flattened summary CSV."
        )
    )
    parser.add_argument("--symbols", nargs="*", help="Optional explicit symbol list.")
    parser.add_argument("--symbols-file", type=Path, help="Optional newline/comma separated symbol file.")
    parser.add_argument("--database-url", default=evaluator.os.environ.get("DATABASE_URL", ""))
    parser.add_argument("--db-statement-timeout-ms", type=int, default=30_000)
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=evaluator.DEFAULT_START_DATE,
        help="Earliest trade date to include in the evaluation window. Defaults to 2015-01-01.",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=evaluator.DEFAULT_END_DATE,
        help="Latest trade date to load. Defaults to today.",
    )
    parser.add_argument(
        "--horizon-bars",
        type=int,
        default=evaluator.DEFAULT_HORIZON_BARS,
        help="Forward trading-bar horizon for the target. Defaults to 5.",
    )
    parser.add_argument(
        "--max-analogs",
        type=int,
        default=None,
        help="Optional override for analog methods. Defaults to each method's built-in analog count.",
    )
    parser.add_argument(
        "--min-candidate-count",
        type=int,
        default=evaluator.DEFAULT_MIN_CANDIDATE_COUNT,
        help="Minimum historical candidates required before emitting a prediction. Defaults to 60.",
    )
    parser.add_argument(
        "--min-spacing-bars",
        type=int,
        default=evaluator.DEFAULT_MIN_SPACING_BARS,
        help="Minimum spacing between selected analog dates, in bars. Defaults to 5.",
    )
    parser.add_argument(
        "--warmup-calendar-days",
        type=int,
        default=evaluator.DEFAULT_WARMUP_CALENDAR_DAYS,
        help="Calendar days of extra history to load before start-date for indicators. Defaults to 120.",
    )
    parser.add_argument(
        "--prediction-method",
        choices=(evaluator.DEFAULT_PREDICTION_METHOD, *evaluator._METHOD_NAMES),
        default=evaluator.DEFAULT_PREDICTION_METHOD,
        help="Prediction method to pass through to the evaluator. Defaults to auto.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="How many symbols to run concurrently. Defaults to 4.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rerun symbols even if a matching per-symbol JSON already exists.",
    )
    parser.add_argument("--run-label", help="Optional batch run label. Defaults to a timestamp.")
    parser.add_argument(
        "--output-suffix",
        default="",
        help="Optional suffix inserted before each per-symbol JSON filename.",
    )
    args = parser.parse_args()
    if not args.symbols and not args.symbols_file:
        raise SystemExit("Provide --symbols and/or --symbols-file.")
    if args.start_date >= args.end_date:
        raise SystemExit("--start-date must be earlier than --end-date.")
    if args.max_workers < 1:
        raise SystemExit("--max-workers must be >= 1.")
    return args


def _normalize_symbol_list(raw_symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in raw_symbols:
        normalized = symbol.strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _load_symbols(args: argparse.Namespace) -> list[str]:
    raw_symbols: list[str] = []
    if args.symbols:
        raw_symbols.extend(args.symbols)
    if args.symbols_file:
        raw_text = args.symbols_file.read_text(encoding="utf-8")
        raw_symbols.extend(chunk for chunk in raw_text.replace("\n", ",").split(","))
    symbols = _normalize_symbol_list(raw_symbols)
    if not symbols:
        raise SystemExit("No symbols were provided after normalization.")
    return symbols


def _result_output_path(
    *,
    results_dir: Path,
    symbol: str,
    start_date: date,
    end_date: date,
    horizon_bars: int,
    prediction_method: str,
    output_suffix: str = "",
) -> Path:
    return results_dir / (
        f"{symbol.lower()}_weekly_price_movement_"
        f"{prediction_method}_h{horizon_bars}_{start_date.isoformat()}_{end_date.isoformat()}{output_suffix}.json"
    )


def _is_completed_output(path: Path, *, args: argparse.Namespace) -> bool:
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if payload.get("target") != f"sign(close[t+{args.horizon_bars}] / close[t] - 1)":
        return False
    if payload.get("horizon_bars") != args.horizon_bars:
        return False
    requested_window = payload.get("requested_window")
    if not isinstance(requested_window, dict):
        return False
    if requested_window.get("start_date") != args.start_date.isoformat():
        return False
    if requested_window.get("end_date") != args.end_date.isoformat():
        return False
    parameters = payload.get("parameters")
    if not isinstance(parameters, dict):
        return False
    if parameters.get("min_candidate_count") != args.min_candidate_count:
        return False
    if parameters.get("min_spacing_bars") != args.min_spacing_bars:
        return False
    if args.prediction_method != evaluator.DEFAULT_PREDICTION_METHOD:
        if payload.get("selected_method") != args.prediction_method:
            return False
    latest_prediction = payload.get("latest_prediction")
    return latest_prediction is None or isinstance(latest_prediction, dict)


def _append_jsonl(path: Path, row: dict[str, object], lock: threading.Lock) -> None:
    line = json.dumps(row, sort_keys=True)
    with lock:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.write("\n")


def _summary_row_from_payload(
    *,
    payload: dict[str, object],
    output_path: Path,
    log_path: Path,
    elapsed_seconds: float,
    status: str,
) -> dict[str, object]:
    evaluation = payload.get("evaluation")
    if not isinstance(evaluation, dict):
        raise ValueError("Payload did not include an evaluation section.")
    parameters = payload.get("parameters")
    if not isinstance(parameters, dict):
        raise ValueError("Payload did not include parameters.")
    latest_prediction = payload.get("latest_prediction")
    latest = latest_prediction if isinstance(latest_prediction, dict) else {}
    return {
        "symbol": payload.get("symbol"),
        "status": status,
        "selected_method": payload.get("selected_method"),
        "selected_method_reason": payload.get("selected_method_reason"),
        "prediction_engine": parameters.get("prediction_engine"),
        "ml_model_name": parameters.get("ml_model_name"),
        "requested_start_date": payload.get("requested_window", {}).get("start_date")
        if isinstance(payload.get("requested_window"), dict)
        else None,
        "requested_end_date": payload.get("requested_window", {}).get("end_date")
        if isinstance(payload.get("requested_window"), dict)
        else None,
        "horizon_bars": payload.get("horizon_bars"),
        "loaded_bar_count": payload.get("loaded_bar_count"),
        "window_bar_count": payload.get("window_bar_count"),
        "accuracy_pct": evaluation.get("accuracy_pct"),
        "balanced_accuracy_pct": evaluation.get("balanced_accuracy_pct"),
        "directional_accuracy_pct": evaluation.get("directional_accuracy_pct"),
        "coverage_pct": evaluation.get("coverage_pct"),
        "observation_count": evaluation.get("observation_count"),
        "total_scorable_dates": evaluation.get("total_scorable_dates"),
        "abstained_count": evaluation.get("abstained_count"),
        "up_precision_pct": evaluation.get("up_precision_pct"),
        "down_precision_pct": evaluation.get("down_precision_pct"),
        "up_recall_pct": evaluation.get("up_recall_pct"),
        "down_recall_pct": evaluation.get("down_recall_pct"),
        "latest_as_of_date": latest.get("as_of_date"),
        "latest_prediction_engine": latest.get("prediction_engine"),
        "latest_direction": latest.get("predicted_direction"),
        "latest_predicted_sign": latest.get("predicted_sign"),
        "latest_confidence_pct": latest.get("confidence_pct"),
        "latest_probability_up_pct": latest.get("probability_up_pct"),
        "latest_probability_down_pct": latest.get("probability_down_pct"),
        "latest_predicted_return_median_pct": latest.get("predicted_return_median_pct"),
        "latest_predicted_return_mean_pct": latest.get("predicted_return_mean_pct"),
        "output_path": str(output_path.relative_to(ROOT)).replace("\\", "/"),
        "log_path": str(log_path.relative_to(ROOT)).replace("\\", "/"),
        "elapsed_seconds": elapsed_seconds,
    }


def _run_symbol(
    *,
    item: SymbolRun,
    args: argparse.Namespace,
    status_jsonl: Path,
    status_lock: threading.Lock,
) -> dict[str, object]:
    start_ts = time.perf_counter()
    if not args.force and _is_completed_output(item.output_path, args=args):
        payload = json.loads(item.output_path.read_text(encoding="utf-8"))
        row = _summary_row_from_payload(
            payload=payload,
            output_path=item.output_path,
            log_path=item.log_path,
            elapsed_seconds=0.0,
            status="skipped_existing",
        )
        _append_jsonl(status_jsonl, row, status_lock)
        return row

    command = [
        sys.executable,
        str(EVALUATOR_SCRIPT),
        "--symbol",
        item.symbol,
        "--database-url",
        args.database_url,
        "--db-statement-timeout-ms",
        str(args.db_statement_timeout_ms),
        "--start-date",
        args.start_date.isoformat(),
        "--end-date",
        args.end_date.isoformat(),
        "--horizon-bars",
        str(args.horizon_bars),
        "--min-candidate-count",
        str(args.min_candidate_count),
        "--min-spacing-bars",
        str(args.min_spacing_bars),
        "--warmup-calendar-days",
        str(args.warmup_calendar_days),
        "--prediction-method",
        args.prediction_method,
        "--output-json",
        str(item.output_path),
    ]
    if args.max_analogs is not None:
        command.extend(["--max-analogs", str(args.max_analogs)])

    item.log_path.parent.mkdir(parents=True, exist_ok=True)
    with item.log_path.open("w", encoding="utf-8") as handle:
        handle.write("COMMAND: " + subprocess.list2cmdline(command) + "\n")
        handle.write(f"STARTED_AT: {datetime.now().isoformat()}\n\n")
        completed = subprocess.run(
            command,
            cwd=str(ROOT),
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        handle.write(f"\nEXIT_CODE: {completed.returncode}\n")

    elapsed = round(time.perf_counter() - start_ts, 3)
    if completed.returncode == 0 and _is_completed_output(item.output_path, args=args):
        payload = json.loads(item.output_path.read_text(encoding="utf-8"))
        row = _summary_row_from_payload(
            payload=payload,
            output_path=item.output_path,
            log_path=item.log_path,
            elapsed_seconds=elapsed,
            status="completed",
        )
        _append_jsonl(status_jsonl, row, status_lock)
        return row

    row = {
        "symbol": item.symbol,
        "status": "failed",
        "requested_start_date": args.start_date.isoformat(),
        "requested_end_date": args.end_date.isoformat(),
        "horizon_bars": args.horizon_bars,
        "prediction_method": args.prediction_method,
        "output_path": str(item.output_path.relative_to(ROOT)).replace("\\", "/"),
        "log_path": str(item.log_path.relative_to(ROOT)).replace("\\", "/"),
        "elapsed_seconds": elapsed,
        "returncode": completed.returncode,
    }
    _append_jsonl(status_jsonl, row, status_lock)
    return row


def _write_summary_csv(*, rows: list[dict[str, object]], path: Path) -> None:
    fieldnames = [
        "symbol",
        "status",
        "selected_method",
        "selected_method_reason",
        "prediction_engine",
        "ml_model_name",
        "requested_start_date",
        "requested_end_date",
        "horizon_bars",
        "loaded_bar_count",
        "window_bar_count",
        "accuracy_pct",
        "balanced_accuracy_pct",
        "directional_accuracy_pct",
        "coverage_pct",
        "observation_count",
        "total_scorable_dates",
        "abstained_count",
        "up_precision_pct",
        "down_precision_pct",
        "up_recall_pct",
        "down_recall_pct",
        "latest_as_of_date",
        "latest_prediction_engine",
        "latest_direction",
        "latest_predicted_sign",
        "latest_confidence_pct",
        "latest_probability_up_pct",
        "latest_probability_down_pct",
        "latest_predicted_return_median_pct",
        "latest_predicted_return_mean_pct",
        "output_path",
        "log_path",
        "elapsed_seconds",
        "returncode",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})


def main() -> int:
    args = _parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required. Provide --database-url or export DATABASE_URL.")
    symbols = _load_symbols(args)

    run_label = args.run_label or datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_dir = LOGS_DIR / "batch" / "weekly_price_movement" / run_label
    results_dir = batch_dir / "results"
    status_jsonl = batch_dir / "status.jsonl"
    summary_csv = batch_dir / "summary.csv"
    results_dir.mkdir(parents=True, exist_ok=True)
    status_lock = threading.Lock()

    runs = [
        SymbolRun(
            symbol=symbol,
            output_path=_result_output_path(
                results_dir=results_dir,
                symbol=symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                horizon_bars=args.horizon_bars,
                prediction_method=args.prediction_method,
                output_suffix=args.output_suffix,
            ),
            log_path=batch_dir / "logs" / f"{symbol.lower()}.log",
        )
        for symbol in symbols
    ]

    print(
        json.dumps(
            {
                "run_label": run_label,
                "symbol_count": len(runs),
                "start_date": args.start_date.isoformat(),
                "end_date": args.end_date.isoformat(),
                "horizon_bars": args.horizon_bars,
                "prediction_method": args.prediction_method,
                "max_analogs": args.max_analogs,
                "min_candidate_count": args.min_candidate_count,
                "min_spacing_bars": args.min_spacing_bars,
                "warmup_calendar_days": args.warmup_calendar_days,
                "max_workers": args.max_workers,
                "status_jsonl": str(status_jsonl.relative_to(ROOT)).replace("\\", "/"),
                "summary_csv": str(summary_csv.relative_to(ROOT)).replace("\\", "/"),
            },
            sort_keys=True,
        )
    )

    results: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                _run_symbol,
                item=item,
                args=args,
                status_jsonl=status_jsonl,
                status_lock=status_lock,
            ): item.symbol
            for item in runs
        }
        for future in as_completed(futures):
            row = future.result()
            results.append(row)
            print(json.dumps(row, sort_keys=True))

    results.sort(key=lambda item: str(item["symbol"]))
    _write_summary_csv(rows=results, path=summary_csv)

    completed_count = sum(1 for row in results if row["status"] == "completed")
    skipped_count = sum(1 for row in results if row["status"] == "skipped_existing")
    failed_count = sum(1 for row in results if row["status"] == "failed")
    print(
        json.dumps(
            {
                "run_label": run_label,
                "completed_count": completed_count,
                "skipped_count": skipped_count,
                "failed_count": failed_count,
                "summary_csv": str(summary_csv.relative_to(ROOT)).replace("\\", "/"),
            },
            sort_keys=True,
        )
    )
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
