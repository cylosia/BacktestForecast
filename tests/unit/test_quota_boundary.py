"""Verify quota enforcement boundary conditions in worker tasks."""
from __future__ import annotations

import inspect


def test_backtest_quota_uses_gte():
    """The backtest quota check must use >= to prevent off-by-one."""
    from apps.worker.app.tasks import run_backtest

    source = inspect.getsource(run_backtest)
    assert "used >= policy.monthly_backtest_quota" in source, (
        "Backtest quota check must use '>=' not '>' to prevent allowing "
        "one extra backtest beyond the quota limit"
    )


def test_sweep_quota_uses_gte():
    """The sweep quota check must use >= to prevent off-by-one."""
    from apps.worker.app.tasks import run_sweep

    source = inspect.getsource(run_sweep)
    assert "sweep_used >= policy.monthly_sweep_quota" in source, (
        "Sweep quota check must use '>=' to prevent allowing "
        "one extra sweep beyond the quota limit"
    )


def test_heartbeat_called_before_backtest_execution():
    """Heartbeat must be updated before starting long-running execution."""
    from apps.worker.app.tasks import run_backtest

    source = inspect.getsource(run_backtest)
    hb_pos = source.find("_update_heartbeat(session, BacktestRun")
    exec_pos = source.find("service.execute_run_by_id")
    assert hb_pos > 0, "Heartbeat call missing from run_backtest"
    assert hb_pos < exec_pos, "Heartbeat must be called before execution"


def test_heartbeat_called_before_scan_execution():
    """Heartbeat must be updated before starting scan execution."""
    from apps.worker.app.tasks import run_scan_job

    source = inspect.getsource(run_scan_job)
    hb_pos = source.find("_update_heartbeat(session, ScannerJobModel")
    exec_pos = source.find("service.run_job")
    assert hb_pos > 0, "Heartbeat call missing from run_scan_job"
    assert hb_pos < exec_pos, "Heartbeat must be called before execution"


def test_heartbeat_called_before_sweep_execution():
    """Heartbeat must be updated before starting sweep execution."""
    from apps.worker.app.tasks import run_sweep

    source = inspect.getsource(run_sweep)
    hb_pos = source.find("_update_heartbeat(session, SweepJobModel")
    exec_pos = source.find("service.run_job")
    assert hb_pos > 0, "Heartbeat call missing from run_sweep"
    assert hb_pos < exec_pos, "Heartbeat must be called before execution"


def test_heartbeat_called_before_analysis_execution():
    """Heartbeat must be updated before starting analysis execution."""
    from apps.worker.app.tasks import run_deep_analysis

    source = inspect.getsource(run_deep_analysis)
    hb_pos = source.find("_update_heartbeat(session, SymbolAnalysis")
    exec_pos = source.find("service.execute_analysis")
    assert hb_pos > 0, "Heartbeat call missing from run_deep_analysis"
    assert hb_pos < exec_pos, "Heartbeat must be called before execution"


def test_task_ownership_allows_running_redelivery():
    """Crashed worker redelivery must be allowed for running jobs."""
    from apps.worker.app.tasks import _validate_task_ownership

    source = inspect.getsource(_validate_task_ownership)
    assert "rejected_running_redelivery" not in source, (
        "Task ownership must not reject redelivery for running jobs; "
        "crashed workers need their tasks re-claimed"
    )
