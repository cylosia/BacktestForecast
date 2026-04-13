from __future__ import annotations

import argparse
import csv
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)
LOGS_DIR = ROOT / "logs"
RESCORE_SCRIPT = ROOT / "scripts" / "export_weekly_calendar_policy_median_rescore_csv.py"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run weekly calendar policy rescoring one symbol at a time with incremental combined output."
    )
    parser.add_argument(
        "--objective",
        choices=("median", "blended"),
        default="blended",
        help="Ranking objective to pass through to the rescore script.",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        help="Optional explicit symbol list.",
    )
    parser.add_argument(
        "--symbols-file",
        type=Path,
        help="Optional newline/comma separated symbol file.",
    )
    parser.add_argument(
        "--recompute-symbols",
        nargs="*",
        default=[],
        help="Optional symbol subset to recompute live instead of using cached trade maps.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Combined CSV output path to refresh after each completed symbol.",
    )
    parser.add_argument(
        "--run-label",
        help="Optional batch run label. Defaults to a timestamp.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rerun symbols even if a per-symbol CSV already exists in the batch directory.",
    )
    return parser.parse_args()


def _load_symbols(args: argparse.Namespace) -> list[str]:
    raw_symbols: list[str] = []
    if args.symbols_file is not None:
        raw_text = args.symbols_file.read_text(encoding="utf-8")
        raw_symbols.extend(chunk.strip().upper() for chunk in raw_text.replace("\n", ",").split(","))
    elif args.symbols:
        raw_symbols.extend(symbol.strip().upper() for symbol in args.symbols)
    else:
        raise SystemExit("No symbols supplied.")

    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in raw_symbols:
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        ordered.append(symbol)
    return ordered


def _read_single_row_csv(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    if len(rows) != 1:
        return None
    return dict(rows[0])


def _write_combined_csv(*, rows: list[dict[str, object]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _run_one_symbol(
    *,
    symbol: str,
    objective: str,
    recompute_symbols: set[str],
    per_symbol_csv: Path,
    per_symbol_log: Path,
) -> int:
    command = [
        sys.executable,
        "-u",
        str(RESCORE_SCRIPT),
        "--objective",
        objective,
        "--output",
        str(per_symbol_csv),
        "--symbols",
        symbol,
    ]
    if symbol in recompute_symbols:
        command.extend(["--recompute-symbols", symbol])

    with per_symbol_log.open("w", encoding="utf-8") as log_handle:
        log_handle.write("COMMAND: " + subprocess.list2cmdline(command) + "\n")
        log_handle.write(f"STARTED_AT: {datetime.now().isoformat()}\n\n")
        log_handle.flush()
        process = subprocess.Popen(
            command,
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        assert process.stdout is not None
        for line in process.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            log_handle.write(line)
            log_handle.flush()
        return_code = process.wait()
        log_handle.write(f"\nFINISHED_AT: {datetime.now().isoformat()}\n")
        log_handle.write(f"RETURN_CODE: {return_code}\n")
        log_handle.flush()
    return return_code


def main() -> int:
    args = _parse_args()
    symbols = _load_symbols(args)
    recompute_symbols = {symbol.upper() for symbol in args.recompute_symbols}
    run_label = args.run_label or datetime.now().strftime("%Y%m%d_%H%M%S")

    batch_dir = LOGS_DIR / "batch" / "weekly_calendar_policy_rescore_one_by_one" / run_label
    rows_dir = batch_dir / "rows"
    logs_dir = batch_dir / "logs"
    rows_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    summary_path = batch_dir / "summary.csv"

    print(
        json.dumps(
            {
                "run_label": run_label,
                "symbol_count": len(symbols),
                "objective": args.objective,
                "output_path": str(args.output).replace("\\", "/"),
                "batch_dir": str(batch_dir.relative_to(ROOT)).replace("\\", "/"),
                "summary_csv": str(summary_path.relative_to(ROOT)).replace("\\", "/"),
            },
            sort_keys=True,
        )
    )

    status_rows: list[dict[str, object]] = []
    combined_rows: list[dict[str, object]] = []

    for index, symbol in enumerate(symbols, start=1):
        per_symbol_csv = rows_dir / f"{symbol.lower()}.csv"
        per_symbol_log = logs_dir / f"{symbol.lower()}.log"
        print(json.dumps({"symbol": symbol, "position": index, "total": len(symbols), "phase": "start"}, sort_keys=True))

        if not args.force:
            existing_row = _read_single_row_csv(per_symbol_csv)
            if existing_row is not None:
                combined_rows.append(existing_row)
                status_rows.append(
                    {
                        "symbol": symbol,
                        "status": "skipped_existing",
                        "row_csv": str(per_symbol_csv.relative_to(ROOT)).replace("\\", "/"),
                        "log_path": str(per_symbol_log.relative_to(ROOT)).replace("\\", "/"),
                    }
                )
                _write_combined_csv(rows=combined_rows, output_path=args.output)
                _write_combined_csv(rows=status_rows, output_path=summary_path)
                print(json.dumps({"symbol": symbol, "phase": "skip_existing"}, sort_keys=True))
                continue

        return_code = _run_one_symbol(
            symbol=symbol,
            objective=args.objective,
            recompute_symbols=recompute_symbols,
            per_symbol_csv=per_symbol_csv,
            per_symbol_log=per_symbol_log,
        )
        row = _read_single_row_csv(per_symbol_csv)
        if return_code != 0 or row is None:
            status_rows.append(
                {
                    "symbol": symbol,
                    "status": "failed",
                    "return_code": return_code,
                    "row_csv": str(per_symbol_csv.relative_to(ROOT)).replace("\\", "/"),
                    "log_path": str(per_symbol_log.relative_to(ROOT)).replace("\\", "/"),
                }
            )
            _write_combined_csv(rows=combined_rows, output_path=args.output)
            _write_combined_csv(rows=status_rows, output_path=summary_path)
            print(json.dumps({"symbol": symbol, "phase": "failed", "return_code": return_code}, sort_keys=True))
            return 1

        combined_rows.append(row)
        status_rows.append(
            {
                "symbol": symbol,
                "status": "completed",
                "row_csv": str(per_symbol_csv.relative_to(ROOT)).replace("\\", "/"),
                "log_path": str(per_symbol_log.relative_to(ROOT)).replace("\\", "/"),
            }
        )
        _write_combined_csv(rows=combined_rows, output_path=args.output)
        _write_combined_csv(rows=status_rows, output_path=summary_path)
        print(json.dumps({"symbol": symbol, "phase": "completed"}, sort_keys=True))

    print(
        json.dumps(
            {
                "run_label": run_label,
                "completed_count": sum(1 for row in status_rows if row.get("status") == "completed"),
                "skipped_count": sum(1 for row in status_rows if row.get("status") == "skipped_existing"),
                "failed_count": sum(1 for row in status_rows if row.get("status") == "failed"),
                "output_path": str(args.output).replace("\\", "/"),
                "summary_csv": str(summary_path.relative_to(ROOT)).replace("\\", "/"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
