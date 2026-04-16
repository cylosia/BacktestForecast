from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

import run_weekly_calendar_policy_two_stage_batch as base  # noqa: E402
from spy_weekly_calendar_policy_1dte_2dte_common import (  # noqa: E402
    DEFAULT_BATCH_RUN_LABEL,
    DEFAULT_SYMBOL,
    DEFAULT_TRAIN_START_DATE,
    REQUESTED_END_DATE,
)


GRID_SCRIPT = ROOT / "scripts" / "grid_search_weekly_calendar_policy_two_stage_spy_1dte_2dte.py"


def _has_flag(argv: list[str], flag: str) -> bool:
    return any(arg == flag or arg.startswith(f"{flag}=") for arg in argv)


def _patched_argv(argv: list[str]) -> list[str]:
    patched = list(argv)
    if not _has_flag(patched, "--symbols") and not _has_flag(patched, "--symbols-file"):
        patched.extend(["--symbols", DEFAULT_SYMBOL])
    if not _has_flag(patched, "--min-start-date"):
        patched.extend(["--min-start-date", DEFAULT_TRAIN_START_DATE.isoformat()])
    if not _has_flag(patched, "--requested-end-date"):
        patched.extend(["--requested-end-date", REQUESTED_END_DATE.isoformat()])
    if not _has_flag(patched, "--objective"):
        patched.extend(["--objective", "median"])
    if not _has_flag(patched, "--run-label"):
        patched.extend(["--run-label", DEFAULT_BATCH_RUN_LABEL])
    if not _has_flag(patched, "--max-workers"):
        patched.extend(["--max-workers", "1"])
    return patched


def _result_output_path(*, symbol: str, start_date: date, requested_end_date: date) -> Path:
    return base.LOGS_DIR / (
        f"{symbol.lower()}_weekly_calendar_policy_two_stage_1dte_2dte_daily_"
        f"{start_date.isoformat()}_{requested_end_date.isoformat()}.json"
    )


def main() -> int:
    base.GRID_SCRIPT = GRID_SCRIPT
    base.DEFAULT_MIN_START_DATE = DEFAULT_TRAIN_START_DATE
    base.DEFAULT_REQUESTED_END_DATE = REQUESTED_END_DATE
    base._result_output_path = _result_output_path
    sys.argv = [sys.argv[0], *_patched_argv(sys.argv[1:])]
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
