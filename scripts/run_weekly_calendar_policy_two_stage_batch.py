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
from sqlalchemy import func

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
LOGS_DIR = ROOT / "logs"
GRID_SCRIPT = ROOT / "scripts" / "grid_search_weekly_calendar_policy_two_stage.py"

from backtestforecast.db.session import create_readonly_session  # noqa: E402
from backtestforecast.models import HistoricalUnderlyingDayBar  # noqa: E402


DEFAULT_MIN_START_DATE = date(2015, 1, 1)
DEFAULT_REQUESTED_END_DATE = date(2026, 4, 2)
DISCOVERY_START_DATE = date(2014, 1, 1)


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
            "Run the weekly calendar two-stage grid search across multiple symbols with "
            "auto-detected start dates and resumable skipping."
        )
    )
    parser.add_argument("--symbols", nargs="*", help="Optional explicit symbol list.")
    parser.add_argument(
        "--symbols-file",
        type=Path,
        help="Optional newline/comma separated symbol file.",
    )
    parser.add_argument(
        "--min-start-date",
        type=date.fromisoformat,
        default=DEFAULT_MIN_START_DATE,
        help="Earliest start date to use. Defaults to 2015-01-01.",
    )
    parser.add_argument(
        "--requested-end-date",
        type=date.fromisoformat,
        default=DEFAULT_REQUESTED_END_DATE,
        help="Requested end date. Defaults to 2026-04-02.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="How many symbols to run concurrently. Defaults to 2.",
    )
    parser.add_argument(
        "--precompute-workers",
        type=int,
        default=2,
        help="Per-symbol precompute worker count passed through to the two-stage runner. Defaults to 2.",
    )
    parser.add_argument(
        "--indicator-workers",
        type=int,
        default=2,
        help="Per-symbol indicator worker count passed through to the two-stage runner. Defaults to 2.",
    )
    parser.add_argument(
        "--objective",
        choices=("average", "median"),
        default="average",
        help="Primary ranking objective passed through to the two-stage runner. Defaults to average ROI on margin.",
    )
    parser.add_argument(
        "--regime-mode",
        choices=("all", "best_regime_only"),
        default="best_regime_only",
        help="Regime selection mode passed through to the two-stage runner. Defaults to best_regime_only.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rerun symbols even if a completed output JSON already exists.",
    )
    parser.add_argument(
        "--run-label",
        help="Optional batch run label. Defaults to a timestamp.",
    )
    parser.add_argument(
        "--output-suffix",
        default="",
        help="Optional suffix inserted before each per-symbol JSON filename.",
    )
    return parser.parse_args()


def _load_symbols(args: argparse.Namespace) -> list[str]:
    raw_symbols: list[str] = []
    if args.symbols:
        raw_symbols.extend(args.symbols)
    if args.symbols_file:
        raw_text = args.symbols_file.read_text(encoding="utf-8")
        for chunk in raw_text.replace("\n", ",").split(","):
            item = chunk.strip().upper()
            if item:
                raw_symbols.append(item)
    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in raw_symbols:
        normalized = symbol.strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    if not ordered:
        raise SystemExit("No symbols supplied.")
    return ordered


def _resolve_symbol_start_dates(
    *,
    symbols: list[str],
    min_start_date: date,
    requested_end_date: date,
) -> dict[str, date]:
    with create_readonly_session() as session:
        rows = (
            session.query(
                HistoricalUnderlyingDayBar.symbol,
                func.min(HistoricalUnderlyingDayBar.trade_date),
            )
            .filter(
                HistoricalUnderlyingDayBar.symbol.in_(symbols),
                HistoricalUnderlyingDayBar.trade_date >= DISCOVERY_START_DATE,
                HistoricalUnderlyingDayBar.trade_date <= requested_end_date,
            )
            .group_by(HistoricalUnderlyingDayBar.symbol)
            .all()
        )
    resolved = {
        str(symbol): max(min_start_date, earliest_trade_date)
        for symbol, earliest_trade_date in rows
        if earliest_trade_date is not None
    }
    return resolved


def _result_output_path(*, symbol: str, start_date: date, requested_end_date: date, output_suffix: str = "") -> Path:
    return LOGS_DIR / (
        f"{symbol.lower()}_weekly_calendar_policy_two_stage_"
        f"{start_date.year}_{requested_end_date.year}{output_suffix}.json"
    )


def _is_completed_output(path: Path, *, objective: str, regime_mode: str) -> bool:
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if "combined_best_result" not in payload:
        return False
    payload_objective = payload.get("selection_objective", "average")
    payload_regime_mode = payload.get("selection_regime_mode", "all")
    return payload_objective == objective and payload_regime_mode == regime_mode


def _append_jsonl(path: Path, row: dict[str, object], lock: threading.Lock) -> None:
    line = json.dumps(row, sort_keys=True)
    with lock:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.write("\n")


def _run_symbol(
    *,
    item: SymbolRun,
    precompute_workers: int,
    indicator_workers: int,
    objective: str,
    regime_mode: str,
    force: bool,
    status_jsonl: Path,
    status_lock: threading.Lock,
) -> dict[str, object]:
    start_ts = time.perf_counter()
    if not force and _is_completed_output(item.output_path, objective=objective, regime_mode=regime_mode):
        payload = json.loads(item.output_path.read_text(encoding="utf-8"))
        best = dict(payload["combined_best_result"])
        row = {
            "symbol": item.symbol,
            "status": "skipped_existing",
            "objective": payload.get("selection_objective", objective),
            "regime_mode": payload.get("selection_regime_mode", regime_mode),
            "start_date": item.start_date.isoformat(),
            "requested_end_date": item.requested_end_date.isoformat(),
            "output_path": str(item.output_path.relative_to(ROOT)).replace("\\", "/"),
            "log_path": str(item.log_path.relative_to(ROOT)).replace("\\", "/"),
            "elapsed_seconds": 0.0,
            "best_stage": best.get("stage"),
            "active_regime": best.get("active_regime"),
            "trade_count": best.get("trade_count"),
            "assignment_count": best.get("assignment_count"),
            "assignment_rate_pct": best.get("assignment_rate_pct"),
            "put_assignment_count": best.get("put_assignment_count"),
            "put_assignment_rate_pct": best.get("put_assignment_rate_pct"),
            "total_net_pnl": best.get("total_net_pnl"),
            "average_roi_on_margin_pct": best.get("average_roi_on_margin_pct"),
            "median_roi_on_margin_pct": best.get("median_roi_on_margin_pct"),
        }
        _append_jsonl(status_jsonl, row, status_lock)
        return row

    command = [
        sys.executable,
        str(GRID_SCRIPT),
        "--symbol",
        item.symbol,
        "--start-date",
        item.start_date.isoformat(),
        "--requested-end-date",
        item.requested_end_date.isoformat(),
        "--output",
        str(item.output_path),
        "--precompute-workers",
        str(precompute_workers),
        "--indicator-workers",
        str(indicator_workers),
        "--objective",
        objective,
        "--regime-mode",
        regime_mode,
    ]
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
    if completed.returncode == 0 and _is_completed_output(item.output_path, objective=objective, regime_mode=regime_mode):
        payload = json.loads(item.output_path.read_text(encoding="utf-8"))
        best = dict(payload["combined_best_result"])
        row = {
            "symbol": item.symbol,
            "status": "completed",
            "objective": payload.get("selection_objective", objective),
            "regime_mode": payload.get("selection_regime_mode", regime_mode),
            "start_date": item.start_date.isoformat(),
            "requested_end_date": item.requested_end_date.isoformat(),
            "output_path": str(item.output_path.relative_to(ROOT)).replace("\\", "/"),
            "log_path": str(item.log_path.relative_to(ROOT)).replace("\\", "/"),
            "elapsed_seconds": elapsed,
            "best_stage": best.get("stage"),
            "active_regime": best.get("active_regime"),
            "trade_count": best.get("trade_count"),
            "assignment_count": best.get("assignment_count"),
            "assignment_rate_pct": best.get("assignment_rate_pct"),
            "put_assignment_count": best.get("put_assignment_count"),
            "put_assignment_rate_pct": best.get("put_assignment_rate_pct"),
            "total_net_pnl": best.get("total_net_pnl"),
            "average_roi_on_margin_pct": best.get("average_roi_on_margin_pct"),
            "median_roi_on_margin_pct": best.get("median_roi_on_margin_pct"),
        }
        _append_jsonl(status_jsonl, row, status_lock)
        return row

    row = {
        "symbol": item.symbol,
        "status": "failed",
        "objective": objective,
        "regime_mode": regime_mode,
        "start_date": item.start_date.isoformat(),
        "requested_end_date": item.requested_end_date.isoformat(),
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
        "regime_mode",
        "start_date",
        "requested_end_date",
        "output_path",
        "log_path",
        "elapsed_seconds",
        "best_stage",
        "active_regime",
        "trade_count",
        "assignment_count",
        "assignment_rate_pct",
        "put_assignment_count",
        "put_assignment_rate_pct",
        "total_net_pnl",
        "average_roi_on_margin_pct",
        "median_roi_on_margin_pct",
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
    start_dates = _resolve_symbol_start_dates(
        symbols=symbols,
        min_start_date=args.min_start_date,
        requested_end_date=args.requested_end_date,
    )
    missing_symbols = [symbol for symbol in symbols if symbol not in start_dates]
    symbols = [symbol for symbol in symbols if symbol in start_dates]
    if not symbols:
        raise SystemExit("No symbols have underlying bars in the requested date window.")

    run_label = args.run_label or datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_dir = LOGS_DIR / "batch" / "weekly_calendar_policy_two_stage" / run_label
    batch_dir.mkdir(parents=True, exist_ok=True)
    status_jsonl = batch_dir / "status.jsonl"
    summary_csv = batch_dir / "summary.csv"
    status_lock = threading.Lock()

    runs = [
        SymbolRun(
            symbol=symbol,
            start_date=start_dates[symbol],
            requested_end_date=args.requested_end_date,
            output_path=_result_output_path(
                symbol=symbol,
                start_date=start_dates[symbol],
                requested_end_date=args.requested_end_date,
                output_suffix=args.output_suffix,
            ),
            log_path=batch_dir / f"{symbol.lower()}.log",
        )
        for symbol in symbols
    ]

    print(
        json.dumps(
            {
                "run_label": run_label,
                "requested_symbol_count": len(symbols) + len(missing_symbols),
                "symbol_count": len(runs),
                "missing_bar_symbol_count": len(missing_symbols),
                "missing_bar_symbols": missing_symbols,
                "objective": args.objective,
                "max_workers": args.max_workers,
                "precompute_workers": args.precompute_workers,
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
                precompute_workers=args.precompute_workers,
                indicator_workers=args.indicator_workers,
                objective=args.objective,
                regime_mode=args.regime_mode,
                force=args.force,
                status_jsonl=status_jsonl,
                status_lock=status_lock,
            ): item.symbol
            for item in runs
        }
        for future in as_completed(futures):
            row = future.result()
            results.append(row)
            print(json.dumps(row, sort_keys=True))

    results.sort(key=lambda item: item["symbol"])
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
