from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path
from uuid import uuid4


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "monitor_historical_import.ps1"
_UNSET = object()


def _status_path() -> Path:
    return REPO_ROOT / f"monitor-status-{uuid4().hex}.json"


def _log_path(kind: str) -> Path:
    return REPO_ROOT / f"monitor-{kind}-{uuid4().hex}.log"


def _logs_dir_path() -> Path:
    return REPO_ROOT / f"monitor-logs-{uuid4().hex}"


def _seed_monitor_status(status_path: Path, **overrides: object) -> None:
    payload: dict[str, object] = {
        "started_at": "2026-03-29T14:02:55-05:00",
        "command": "python scripts/sync_historical_market_data.py --start-date 2025-04-01 --end-date 2025-04-02",
        "status_path": str(status_path),
    }
    payload.update(overrides)
    status_path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_monitor_bundle(
    status_path: Path,
    *,
    stdout_kind: str | None = None,
    stdout_content: object = _UNSET,
    stderr_kind: str | None = None,
    stderr_content: object = _UNSET,
    **status_overrides: object,
) -> tuple[Path | None, Path | None]:
    stdout_path = _log_path(stdout_kind) if stdout_kind is not None else None
    stderr_path = _log_path(stderr_kind) if stderr_kind is not None else None

    if stdout_path is not None:
        status_overrides.setdefault("stdout_log_path", str(stdout_path))
        if stdout_content is not _UNSET:
            stdout_path.write_text(str(stdout_content), encoding="utf-8")
    if stderr_path is not None:
        status_overrides.setdefault("stderr_log_path", str(stderr_path))
        if stderr_content is not _UNSET:
            stderr_path.write_text(str(stderr_content), encoding="utf-8")

    _seed_monitor_status(status_path, **status_overrides)
    return stdout_path, stderr_path


def _run_monitor(status_path: Path) -> str:
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-File",
            str(SCRIPT_PATH),
            "-StatusPath",
            str(status_path),
            "-Once",
            "-NoClear",
            "-SkipDatabaseSummary",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def _run_monitor_with_args(status_path: Path, *extra_args: str) -> str:
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-File",
            str(SCRIPT_PATH),
            "-StatusPath",
            str(status_path),
            "-Once",
            "-NoClear",
            "-SkipDatabaseSummary",
            *extra_args,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def _run_monitor_without_status_path(logs_dir: Path, status_pattern: str, *extra_args: str) -> str:
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-File",
            str(SCRIPT_PATH),
            "-LogsDir",
            str(logs_dir),
            "-StatusPattern",
            status_pattern,
            "-Once",
            "-NoClear",
            "-SkipDatabaseSummary",
            *extra_args,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def _run_monitor_result(status_path: Path, *extra_args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-File",
            str(SCRIPT_PATH),
            "-StatusPath",
            str(status_path),
            "-Once",
            "-NoClear",
            "-SkipDatabaseSummary",
            *extra_args,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_monitor_script_tolerates_legacy_status_files_without_progress_fields() -> None:
    status_path = _status_path()
    try:
        _seed_monitor_status(status_path)

        output = _run_monitor(status_path)

        assert "Status file:" in output
        assert "Window: 2025-04-01 -> 2025-04-02" in output
        assert "Import status:" not in output
        assert "Progress:" not in output
        assert "Last completed trade date:" not in output
    finally:
        status_path.unlink(missing_ok=True)


def test_monitor_script_reports_malformed_status_json_clearly() -> None:
    status_path = _status_path()
    try:
        status_path.write_text("{not-json", encoding="utf-8")

        result = _run_monitor_result(status_path)

        assert result.returncode != 0
        assert "Status file contains invalid JSON:" in result.stderr
        assert str(status_path) in result.stderr
    finally:
        status_path.unlink(missing_ok=True)


def test_monitor_script_shows_progress_fields_when_present() -> None:
    status_path = _status_path()
    try:
        _seed_monitor_status(
            status_path,
            status="running",
            completed_trade_dates=3,
            total_trade_dates=10,
            completed_pct=30.0,
            last_completed_trade_date="2025-04-01",
            completed_stock_rows=120,
            completed_option_rows=450,
            updated_at="2026-03-29T14:30:00-05:00",
            error="transient warning",
        )

        output = _run_monitor(status_path)

        assert "Import status: running" in output
        assert re.search(r"Progress: 3/10 trade dates \(30(?:\.0+)?%\)", output)
        assert "Last completed trade date: 2025-04-01" in output
        assert "Completed rows: stock=120 option=450" in output
        assert "Last status update: 2026-03-29T14:30:00-05:00" in output
        assert "Last error: transient warning" in output
    finally:
        status_path.unlink(missing_ok=True)


def test_monitor_script_shows_partial_progress_fields_when_counts_are_incomplete() -> None:
    status_path = _status_path()
    try:
        _seed_monitor_status(
            status_path,
            status="running",
            completed_pct=30.0,
        )

        output = _run_monitor(status_path)

        assert "Import status: running" in output
        assert re.search(r"Progress: \?/\? trade dates \(30(?:\.0+)?%\)", output)
    finally:
        status_path.unlink(missing_ok=True)


def test_monitor_script_reports_missing_log_files() -> None:
    status_path = _status_path()
    missing_stdout, missing_stderr = _seed_monitor_bundle(
        status_path,
        stdout_kind="missing-stdout",
        stderr_kind="missing-stderr",
    )
    try:
        output = _run_monitor(status_path)

        assert f"Missing log file: {missing_stdout}" in output
        assert f"Missing log file: {missing_stderr}" in output
    finally:
        status_path.unlink(missing_ok=True)


def test_monitor_script_tails_existing_log_files() -> None:
    status_path = _status_path()
    stdout_path, stderr_path = _seed_monitor_bundle(
        status_path,
        stdout_kind="stdout",
        stdout_content="stdout first\nstdout second\nstdout third\n",
        stderr_kind="stderr",
        stderr_content="stderr only\n",
    )
    try:
        output = _run_monitor_with_args(status_path, "-TailLines", "2")

        assert f"Path: {stdout_path}" in output
        assert f"Path: {stderr_path}" in output
        assert "stdout first" not in output
        assert "stdout second" in output
        assert "stdout third" in output
        assert "stderr only" in output
    finally:
        status_path.unlink(missing_ok=True)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_monitor_script_tails_only_last_line_when_tail_lines_is_one() -> None:
    status_path = _status_path()
    stdout_path, stderr_path = _seed_monitor_bundle(
        status_path,
        stdout_kind="tail1-stdout",
        stdout_content="stdout first\nstdout second\nstdout third\n",
        stderr_kind="tail1-stderr",
        stderr_content="stderr first\nstderr second\n",
    )
    try:
        output = _run_monitor_with_args(status_path, "-TailLines", "1")

        assert "stdout first" not in output
        assert "stdout second" not in output
        assert "stdout third" in output
        assert "stderr first" not in output
        assert "stderr second" in output
    finally:
        status_path.unlink(missing_ok=True)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_monitor_script_treats_tail_lines_zero_as_empty_tail() -> None:
    status_path = _status_path()
    stdout_path, stderr_path = _seed_monitor_bundle(
        status_path,
        stdout_kind="tail0-stdout",
        stdout_content="stdout first\nstdout second\n",
        stderr_kind="tail0-stderr",
        stderr_content="stderr first\nstderr second\n",
    )
    try:
        output = _run_monitor_with_args(status_path, "-TailLines", "0")

        assert "stdout first" not in output
        assert "stdout second" not in output
        assert "stderr first" not in output
        assert "stderr second" not in output
        assert output.count("(empty)") >= 2
    finally:
        status_path.unlink(missing_ok=True)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_monitor_script_reports_not_running_process_when_pid_is_present() -> None:
    status_path = _status_path()
    try:
        _seed_monitor_status(
            status_path,
            python_pid=999999,
        )

        output = _run_monitor(status_path)

        assert "Process" in output
        assert "NOT RUNNING  PID=999999" in output
    finally:
        status_path.unlink(missing_ok=True)


def test_monitor_script_prefers_explicit_window_over_command_parsing() -> None:
    status_path = _status_path()
    try:
        _seed_monitor_status(
            status_path,
            command="python scripts/sync_historical_market_data.py --start-date 2025-04-01 --end-date 2025-04-02",
        )

        output = _run_monitor_with_args(
            status_path,
            "-StartDate", "2025-05-01",
            "-EndDate", "2025-05-02",
        )

        assert "Window: 2025-05-01 -> 2025-05-02" in output
        assert "Window: 2025-04-01 -> 2025-04-02" not in output
    finally:
        status_path.unlink(missing_ok=True)


def test_monitor_script_uses_explicit_start_date_with_command_derived_end_date() -> None:
    status_path = _status_path()
    try:
        _seed_monitor_status(
            status_path,
            command="python scripts/sync_historical_market_data.py --start-date 2025-04-01 --end-date 2025-04-02",
        )

        output = _run_monitor_with_args(
            status_path,
            "-StartDate", "2025-05-01",
        )

        assert "Window: 2025-05-01 -> 2025-04-02" in output
    finally:
        status_path.unlink(missing_ok=True)


def test_monitor_script_uses_command_derived_start_date_with_explicit_end_date() -> None:
    status_path = _status_path()
    try:
        _seed_monitor_status(
            status_path,
            command="python scripts/sync_historical_market_data.py --start-date 2025-04-01 --end-date 2025-04-02",
        )

        output = _run_monitor_with_args(
            status_path,
            "-EndDate", "2025-05-02",
        )

        assert "Window: 2025-04-01 -> 2025-05-02" in output
    finally:
        status_path.unlink(missing_ok=True)


def test_monitor_script_shows_window_unavailable_without_command_or_overrides() -> None:
    status_path = _status_path()
    try:
        _seed_monitor_status(
            status_path,
            command="",
        )

        output = _run_monitor(status_path)

        assert "Command: " in output
        assert "Window: unavailable" in output
    finally:
        status_path.unlink(missing_ok=True)


def test_monitor_script_shows_window_unavailable_and_process_state_without_command() -> None:
    status_path = _status_path()
    try:
        _seed_monitor_status(
            status_path,
            command="",
            python_pid=999999,
        )

        output = _run_monitor(status_path)

        assert "Window: unavailable" in output
        assert "NOT RUNNING  PID=999999" in output
    finally:
        status_path.unlink(missing_ok=True)


def test_monitor_script_shows_empty_for_zero_byte_log_files() -> None:
    status_path = _status_path()
    stdout_path, stderr_path = _seed_monitor_bundle(
        status_path,
        stdout_kind="empty-stdout",
        stdout_content="",
        stderr_kind="empty-stderr",
        stderr_content="",
    )
    try:
        output = _run_monitor(status_path)

        assert f"Path: {stdout_path}" in output
        assert f"Path: {stderr_path}" in output
        assert output.count("(empty)") >= 2
    finally:
        status_path.unlink(missing_ok=True)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_monitor_script_handles_only_one_log_path_present() -> None:
    status_path = _status_path()
    stdout_path, _ = _seed_monitor_bundle(
        status_path,
        stdout_kind="single-stdout",
        stdout_content="stdout only\n",
    )
    try:
        output = _run_monitor(status_path)

        assert f"Path: {stdout_path}" in output
        assert "stdout only" in output
        assert output.count("No log path configured.") >= 1
    finally:
        status_path.unlink(missing_ok=True)
        stdout_path.unlink(missing_ok=True)


def test_monitor_script_uses_latest_status_file_from_logs_dir_when_status_path_is_omitted() -> None:
    logs_dir = _logs_dir_path()
    status_path = logs_dir / "historical_import_single.status.json"
    try:
        logs_dir.mkdir()
        _seed_monitor_status(status_path, command="")

        output = _run_monitor_without_status_path(logs_dir, "historical_import_*.status.json")

        assert f"Status file: {status_path}" in output
        assert "Window: unavailable" in output
    finally:
        status_path.unlink(missing_ok=True)
        logs_dir.rmdir()


def test_monitor_script_prefers_most_recent_matching_status_file_from_logs_dir() -> None:
    logs_dir = _logs_dir_path()
    older_status_path = logs_dir / "historical_import_older.status.json"
    newer_status_path = logs_dir / "historical_import_newer.status.json"
    ignored_status_path = logs_dir / "ignored.status.json"
    ignored_extension_path = logs_dir / "historical_import_newest.status.txt"
    try:
        logs_dir.mkdir()
        _seed_monitor_status(older_status_path, command="")
        time.sleep(0.05)
        _seed_monitor_status(newer_status_path, command="")
        _seed_monitor_status(ignored_status_path, command="python ignored.py")
        time.sleep(0.05)
        _seed_monitor_status(ignored_extension_path, command="python ignored.py")

        output = _run_monitor_without_status_path(logs_dir, "historical_import_*.status.json")

        assert f"Status file: {newer_status_path}" in output
        assert f"Status file: {older_status_path}" not in output
        assert f"Status file: {ignored_status_path}" not in output
        assert f"Status file: {ignored_extension_path}" not in output
    finally:
        older_status_path.unlink(missing_ok=True)
        newer_status_path.unlink(missing_ok=True)
        ignored_status_path.unlink(missing_ok=True)
        ignored_extension_path.unlink(missing_ok=True)
        logs_dir.rmdir()
