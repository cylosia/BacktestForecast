from __future__ import annotations

import argparse
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
        description="Run the weekly calendar policy rescore script with tee-style logging."
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
        help="CSV output path for the rescore results.",
    )
    parser.add_argument(
        "--run-label",
        help="Optional batch run label. Defaults to a timestamp.",
    )
    return parser.parse_args()


def _build_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        "-u",
        str(RESCORE_SCRIPT),
        "--objective",
        args.objective,
        "--output",
        str(args.output),
    ]
    if args.symbols_file is not None:
        command.extend(["--symbols-file", str(args.symbols_file)])
    elif args.symbols:
        command.append("--symbols")
        command.extend(args.symbols)
    if args.recompute_symbols:
        command.append("--recompute-symbols")
        command.extend(symbol.upper() for symbol in args.recompute_symbols)
    return command


def main() -> int:
    args = _parse_args()
    run_label = args.run_label or datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_dir = LOGS_DIR / "batch" / "weekly_calendar_policy_rescore" / run_label
    batch_dir.mkdir(parents=True, exist_ok=True)
    log_path = batch_dir / "stdout.log"
    metadata_path = batch_dir / "metadata.json"
    command = _build_command(args)

    start_metadata = {
        "run_label": run_label,
        "started_at": datetime.now().isoformat(),
        "command": command,
        "cwd": str(ROOT),
        "log_path": str(log_path.relative_to(ROOT)).replace("\\", "/"),
        "output_path": str(args.output).replace("\\", "/"),
    }
    metadata_path.write_text(json.dumps(start_metadata, indent=2), encoding="utf-8")
    print(json.dumps(start_metadata, sort_keys=True))

    with log_path.open("w", encoding="utf-8") as log_handle:
        log_handle.write("COMMAND: " + subprocess.list2cmdline(command) + "\n")
        log_handle.write(f"STARTED_AT: {start_metadata['started_at']}\n\n")
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
        try:
            assert process.stdout is not None
            for line in process.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                log_handle.write(line)
                log_handle.flush()
            return_code = process.wait()
        except KeyboardInterrupt:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            raise
        finally:
            finished_at = datetime.now().isoformat()
            log_handle.write(f"\nFINISHED_AT: {finished_at}\n")
            log_handle.write(f"RETURN_CODE: {process.returncode}\n")
            log_handle.flush()

    final_metadata = {
        **start_metadata,
        "finished_at": finished_at,
        "return_code": return_code,
    }
    metadata_path.write_text(json.dumps(final_metadata, indent=2), encoding="utf-8")
    print(json.dumps(final_metadata, sort_keys=True))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
