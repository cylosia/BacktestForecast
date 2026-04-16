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
EVALUATOR_SCRIPT = ROOT / "scripts" / "evaluate_five_regime_price_predictions.py"

import evaluate_five_regime_price_predictions as evaluator  # noqa: E402


@dataclass(frozen=True, slots=True)
class SymbolRun:
    symbol: str
    start_date: date
    requested_end_date: date
    output_path: Path
    log_path: Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the five-regime price-prediction evaluator across multiple symbols with "
            "auto-detected start dates and resumable per-symbol outputs."
        )
    )
    parser.add_argument("--symbols", nargs="*", help="Optional explicit symbol list.")
    parser.add_argument("--symbols-file", type=Path, help="Optional newline/comma separated symbol file.")
    parser.add_argument(
        "--min-start-date",
        type=date.fromisoformat,
        default=evaluator.DEFAULT_MIN_START_DATE,
        help="Earliest start date to use. Defaults to 2015-01-01.",
    )
    parser.add_argument(
        "--requested-end-date",
        type=date.fromisoformat,
        default=evaluator.DEFAULT_REQUESTED_END_DATE,
        help="Requested end date. Defaults to 2026-04-02.",
    )
    parser.add_argument(
        "--forward-weeks",
        type=int,
        default=1,
        help="How many Friday-to-Friday steps ahead to score. Defaults to 1.",
    )
    parser.add_argument(
        "--neutral-move-pct",
        type=float,
        default=1.0,
        help="Fallback neutral threshold when --neutral-move-pcts is not supplied. Defaults to 1.0.",
    )
    parser.add_argument(
        "--heavy-move-pct",
        type=float,
        default=3.0,
        help="Fallback heavy threshold when --heavy-move-pcts is not supplied. Defaults to 3.0.",
    )
    parser.add_argument("--neutral-move-pcts", help="Optional comma-separated neutral thresholds to sweep.")
    parser.add_argument("--heavy-move-pcts", help="Optional comma-separated heavy thresholds to sweep.")
    parser.add_argument(
        "--ema-gap-threshold-pcts",
        default=evaluator.DEFAULT_EMA_GAP_THRESHOLD_PCTS,
        help="Comma-separated EMA-gap thresholds to pass through to the evaluator.",
    )
    parser.add_argument(
        "--heavy-vol-threshold-pcts",
        default=evaluator.DEFAULT_HEAVY_VOL_THRESHOLD_PCTS,
        help="Comma-separated heavy-volatility thresholds to pass through to the evaluator.",
    )
    parser.add_argument(
        "--objective",
        choices=tuple(evaluator.OBJECTIVE_FIELD_MAP),
        default=evaluator.DEFAULT_OBJECTIVE,
        help="Primary ranking metric passed through to the evaluator.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=evaluator.DEFAULT_TOP_K,
        help="How many top rows per symbol to keep in the output. Defaults to 10.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=52,
        help="Minimum number of scored Fridays required per symbol. Defaults to 52.",
    )
    parser.add_argument(
        "--min-predicted-regime-count",
        type=int,
        default=evaluator.DEFAULT_MIN_PREDICTED_REGIME_COUNT,
        help="Minimum predicted count required for each regime before a candidate is constraint-passing.",
    )
    parser.add_argument(
        "--allow-non-monotonic-forward-returns",
        action="store_true",
        help="Disable the monotonic-forward-return constraint in the evaluator.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="How many symbols to run concurrently. Defaults to 2.",
    )
    parser.add_argument(
        "--indicator-workers",
        type=int,
        default=evaluator.two_stage.DEFAULT_INDICATOR_WORKERS,
        help="Per-symbol indicator worker count passed through to the evaluator.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rerun symbols even if a completed per-symbol JSON already exists.",
    )
    parser.add_argument(
        "--disable-cache",
        action="store_true",
        help="Disable indicator-cache reuse in per-symbol evaluator runs.",
    )
    parser.add_argument("--run-label", help="Optional batch run label. Defaults to a timestamp.")
    parser.add_argument(
        "--output-suffix",
        default="",
        help="Optional suffix inserted before each per-symbol JSON filename.",
    )
    return parser.parse_args()


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
        raw_symbols.extend(chunk.strip().upper() for chunk in raw_text.replace("\n", ",").split(","))
    ordered = _normalize_symbol_list(raw_symbols)
    if ordered:
        return ordered
    discovered = evaluator._discover_symbols(
        min_start_date=args.min_start_date,
        requested_end_date=args.requested_end_date,
    )
    if not discovered:
        raise SystemExit("No symbols found with underlying bars in the requested date window.")
    return discovered


def _format_token(value: object) -> str:
    token = str(value).strip().lower()
    return token.replace("-", "m").replace(".", "p").replace(",", "_")


def _result_output_path(
    *,
    results_dir: Path,
    symbol: str,
    forward_weeks: int,
    objective: str,
    output_suffix: str = "",
) -> Path:
    return results_dir / (
        f"{symbol.lower()}_five_regime_price_predictions_"
        f"w{forward_weeks}_{objective}{output_suffix}.json"
    )


def _is_completed_output(path: Path, *, objective: str) -> bool:
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if payload.get("objective") != objective:
        return False
    symbols = payload.get("symbols")
    if not isinstance(symbols, list) or len(symbols) != 1:
        return False
    best_result = symbols[0].get("best_result") if isinstance(symbols[0], dict) else None
    return isinstance(best_result, dict)


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
    symbol_rows = payload.get("symbols")
    if not isinstance(symbol_rows, list) or len(symbol_rows) != 1 or not isinstance(symbol_rows[0], dict):
        raise ValueError("Expected exactly one symbol result in the payload.")
    symbol_row = dict(symbol_rows[0])
    best_result = symbol_row.get("best_result")
    if not isinstance(best_result, dict):
        raise ValueError("Payload did not include a best_result row.")

    return {
        "symbol": symbol_row.get("symbol"),
        "status": status,
        "objective": payload.get("objective"),
        "start_date": symbol_row.get("start_date"),
        "latest_available_date": symbol_row.get("latest_available_date"),
        "requested_end_date": symbol_row.get("requested_end_date"),
        "forward_weeks": payload.get("forward_weeks"),
        "threshold_config_count": len(payload.get("threshold_configs", []) or []),
        "feature_gate_count": len(payload.get("feature_gate_configs", []) or []),
        "output_path": str(output_path.relative_to(ROOT)).replace("\\", "/"),
        "log_path": str(log_path.relative_to(ROOT)).replace("\\", "/"),
        "elapsed_seconds": elapsed_seconds,
        "observation_count": symbol_row.get("observation_count"),
        "scored_config_count": symbol_row.get("scored_config_count"),
        "constraint_passing_config_count": symbol_row.get("constraint_passing_config_count"),
        "best_result_selection": symbol_row.get("best_result_selection"),
        "indicator_periods": best_result.get("indicator_periods"),
        "roc_period": best_result.get("roc_period"),
        "adx_period": best_result.get("adx_period"),
        "rsi_period": best_result.get("rsi_period"),
        "bull_filter": best_result.get("bull_filter"),
        "bear_filter": best_result.get("bear_filter"),
        "threshold_config": best_result.get("threshold_config"),
        "neutral_move_pct": best_result.get("neutral_move_pct"),
        "heavy_move_pct": best_result.get("heavy_move_pct"),
        "feature_gate": best_result.get("feature_gate"),
        "ema_gap_threshold_pct": best_result.get("ema_gap_threshold_pct"),
        "heavy_vol_threshold_pct": best_result.get("heavy_vol_threshold_pct"),
        "constraint_passed": best_result.get("constraint_passed"),
        "constraint_fail_reasons": (
            ";".join(best_result.get("constraint_fail_reasons", []))
            if isinstance(best_result.get("constraint_fail_reasons"), list)
            else best_result.get("constraint_fail_reasons")
        ),
        "exact_accuracy_pct": best_result.get("exact_accuracy_pct"),
        "directional_accuracy_pct": best_result.get("directional_accuracy_pct"),
        "balanced_accuracy_pct": best_result.get("balanced_accuracy_pct"),
        "macro_f1_pct": best_result.get("macro_f1_pct"),
        "macro_precision_pct": best_result.get("macro_precision_pct"),
        "macro_recall_pct": best_result.get("macro_recall_pct"),
        "exact_hit_count": best_result.get("exact_hit_count"),
        "directional_hit_count": best_result.get("directional_hit_count"),
    }


def _run_symbol(
    *,
    item: SymbolRun,
    args: argparse.Namespace,
    status_jsonl: Path,
    status_lock: threading.Lock,
) -> dict[str, object]:
    start_ts = time.perf_counter()
    if not args.force and _is_completed_output(item.output_path, objective=args.objective):
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
        "--symbols",
        item.symbol,
        "--min-start-date",
        item.start_date.isoformat(),
        "--requested-end-date",
        item.requested_end_date.isoformat(),
        "--forward-weeks",
        str(args.forward_weeks),
        "--neutral-move-pct",
        str(args.neutral_move_pct),
        "--heavy-move-pct",
        str(args.heavy_move_pct),
        "--ema-gap-threshold-pcts",
        str(args.ema_gap_threshold_pcts),
        "--heavy-vol-threshold-pcts",
        str(args.heavy_vol_threshold_pcts),
        "--objective",
        args.objective,
        "--top-k",
        str(args.top_k),
        "--min-observations",
        str(args.min_observations),
        "--min-predicted-regime-count",
        str(args.min_predicted_regime_count),
        "--indicator-workers",
        str(args.indicator_workers),
        "--output-json",
        str(item.output_path),
    ]
    if args.neutral_move_pcts:
        command.extend(["--neutral-move-pcts", str(args.neutral_move_pcts)])
    if args.heavy_move_pcts:
        command.extend(["--heavy-move-pcts", str(args.heavy_move_pcts)])
    if args.allow_non_monotonic_forward_returns:
        command.append("--allow-non-monotonic-forward-returns")
    if args.disable_cache:
        command.append("--disable-cache")

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
    if completed.returncode == 0 and _is_completed_output(item.output_path, objective=args.objective):
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
        "objective": args.objective,
        "start_date": item.start_date.isoformat(),
        "requested_end_date": item.requested_end_date.isoformat(),
        "forward_weeks": args.forward_weeks,
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
        "objective",
        "start_date",
        "latest_available_date",
        "requested_end_date",
        "forward_weeks",
        "threshold_config_count",
        "feature_gate_count",
        "output_path",
        "log_path",
        "elapsed_seconds",
        "observation_count",
        "scored_config_count",
        "constraint_passing_config_count",
        "best_result_selection",
        "indicator_periods",
        "roc_period",
        "adx_period",
        "rsi_period",
        "bull_filter",
        "bear_filter",
        "threshold_config",
        "neutral_move_pct",
        "heavy_move_pct",
        "feature_gate",
        "ema_gap_threshold_pct",
        "heavy_vol_threshold_pct",
        "constraint_passed",
        "constraint_fail_reasons",
        "exact_accuracy_pct",
        "directional_accuracy_pct",
        "balanced_accuracy_pct",
        "macro_f1_pct",
        "macro_precision_pct",
        "macro_recall_pct",
        "exact_hit_count",
        "directional_hit_count",
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
    symbols = _load_symbols(args)
    start_dates = evaluator._resolve_symbol_start_dates(
        symbols=symbols,
        min_start_date=args.min_start_date,
        requested_end_date=args.requested_end_date,
    )
    missing_symbols = [symbol for symbol in symbols if symbol not in start_dates]
    symbols = [symbol for symbol in symbols if symbol in start_dates]
    if not symbols:
        raise SystemExit("No symbols have underlying bars in the requested date window.")

    run_label = args.run_label or datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_dir = LOGS_DIR / "batch" / "five_regime_price_predictions" / run_label
    results_dir = batch_dir / "results"
    status_jsonl = batch_dir / "status.jsonl"
    summary_csv = batch_dir / "summary.csv"
    results_dir.mkdir(parents=True, exist_ok=True)
    status_lock = threading.Lock()

    runs = [
        SymbolRun(
            symbol=symbol,
            start_date=start_dates[symbol],
            requested_end_date=args.requested_end_date,
            output_path=_result_output_path(
                results_dir=results_dir,
                symbol=symbol,
                forward_weeks=args.forward_weeks,
                objective=args.objective,
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
                "requested_symbol_count": len(symbols) + len(missing_symbols),
                "symbol_count": len(runs),
                "explicit_symbol_mode": bool(args.symbols or args.symbols_file),
                "missing_bar_symbol_count": len(missing_symbols),
                "missing_bar_symbols": missing_symbols,
                "objective": args.objective,
                "forward_weeks": args.forward_weeks,
                "neutral_move_pct": args.neutral_move_pct,
                "heavy_move_pct": args.heavy_move_pct,
                "neutral_move_pcts": args.neutral_move_pcts,
                "heavy_move_pcts": args.heavy_move_pcts,
                "ema_gap_threshold_pcts": args.ema_gap_threshold_pcts,
                "heavy_vol_threshold_pcts": args.heavy_vol_threshold_pcts,
                "min_predicted_regime_count": args.min_predicted_regime_count,
                "require_monotonic_forward_returns": not args.allow_non_monotonic_forward_returns,
                "max_workers": args.max_workers,
                "indicator_workers": args.indicator_workers,
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
                "objective": args.objective,
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
