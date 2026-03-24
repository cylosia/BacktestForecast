from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from backtestforecast.services.job_transitions import (
    cancellation_blocked_message,
    deletion_blocked_message,
    fail_job_if_active,
    running_transition_values,
    transition_job_to_running,
)


def test_transition_job_to_running_rejects_terminal_status() -> None:
    job = SimpleNamespace(status="succeeded", started_at=None, updated_at=None)

    with pytest.raises(ValueError, match="terminal status"):
        transition_job_to_running(job)


def test_fail_job_if_active_does_not_mutate_terminal_job() -> None:
    job = SimpleNamespace(
        status="cancelled",
        error_code="cancelled_by_user",
        error_message="Cancelled by user.",
        completed_at="done",
        updated_at="done",
    )

    changed = fail_job_if_active(
        job,
        error_code="internal_error",
        error_message="should not apply",
    )

    assert changed is False
    assert job.status == "cancelled"
    assert job.error_code == "cancelled_by_user"


def test_running_transition_values_clear_previous_terminal_fields() -> None:
    values = running_transition_values()

    assert values["status"] == "running"
    assert values["completed_at"] is None
    assert values["error_code"] is None
    assert values["error_message"] is None


def test_delete_and_cancel_messages_are_actionable() -> None:
    delete_message = deletion_blocked_message("export job")
    cancel_message = cancellation_blocked_message("export job")

    assert "Use cancel first" in delete_message
    assert "terminal state" in delete_message
    assert "Refresh the job first" in cancel_message


def test_user_visible_job_conflicts_use_shared_actionable_messages() -> None:
    expectations = {
        "src/backtestforecast/services/backtests.py": ['deletion_blocked_message("backtest run")', 'cancellation_blocked_message("backtest run")'],
        "src/backtestforecast/services/exports.py": ['deletion_blocked_message("export job")', 'cancellation_blocked_message("export job")'],
        "src/backtestforecast/services/scans.py": ['deletion_blocked_message("scanner job")', 'cancellation_blocked_message("scanner job")'],
        "src/backtestforecast/services/sweeps.py": ['deletion_blocked_message("sweep job")', 'cancellation_blocked_message("sweep job")'],
        "src/backtestforecast/pipeline/deep_analysis.py": ['deletion_blocked_message("analysis")', 'cancellation_blocked_message("analysis")'],
    }
    for path, snippets in expectations.items():
        source = Path(path).read_text(encoding="utf-8")
        for snippet in snippets:
            assert snippet in source, (path, snippet)


def test_state_machine_documentation_covers_all_async_job_types() -> None:
    text = Path("docs/job-state-machines.md").read_text(encoding="utf-8")

    for heading in (
        "## Backtest Runs",
        "## Export Jobs",
        "## Scanner Jobs",
        "## Sweep Jobs",
        "## Deep Analyses",
        "## Worker Resource Ownership",
    ):
        assert heading in text


def test_worker_tasks_use_shared_owned_resource_close_helper() -> None:
    source = Path("apps/worker/app/tasks.py").read_text(encoding="utf-8")

    assert "close_owned_resource as _close_owned_resource" in source
    for label in (
        "nightly_scan.market_data_service",
        "nightly_scan.execution_service",
        "nightly_scan.executor",
        "nightly_scan.massive_client",
        "backtests.run.service",
        "exports.generate.service",
        "analysis.market_data_service",
        "analysis.executor",
        "analysis.massive_client",
        "scans.run_job.service",
        "sweeps.run.service",
        "scans.refresh_prioritized.service",
    ):
        assert label in source
