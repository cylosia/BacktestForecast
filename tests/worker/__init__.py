"""Structural tests for Celery task definitions.

These tests verify task registration, signatures, and error handling
without deep mocking.
"""
from __future__ import annotations

import inspect


class TestTaskRegistration:
    """Verify all expected Celery tasks are registered."""

    def test_run_backtest_task_exists(self):
        from apps.worker.app.tasks import run_backtest
        assert callable(run_backtest)

    def test_run_scan_job_task_exists(self):
        from apps.worker.app.tasks import run_scan_job
        assert callable(run_scan_job)

    def test_run_deep_analysis_task_exists(self):
        from apps.worker.app.tasks import run_deep_analysis
        assert callable(run_deep_analysis)

    def test_generate_export_task_exists(self):
        from apps.worker.app.tasks import generate_export
        assert callable(generate_export)

    def test_reap_stale_jobs_task_exists(self):
        from apps.worker.app.tasks import reap_stale_jobs
        assert callable(reap_stale_jobs)

    def test_ping_task_exists(self):
        from apps.worker.app.tasks import ping
        assert callable(ping)

    def test_nightly_scan_pipeline_task_exists(self):
        from apps.worker.app.tasks import nightly_scan_pipeline
        assert callable(nightly_scan_pipeline)

    def test_refresh_prioritized_scans_task_exists(self):
        from apps.worker.app.tasks import refresh_prioritized_scans
        assert callable(refresh_prioritized_scans)


class TestTaskSignatures:
    """Verify task function signatures accept expected arguments."""

    def test_run_backtest_accepts_run_id(self):
        from apps.worker.app.tasks import run_backtest
        sig = inspect.signature(run_backtest)
        params = list(sig.parameters.keys())
        assert "run_id" in params, "run_backtest should accept run_id"

    def test_run_scan_job_accepts_job_id(self):
        from apps.worker.app.tasks import run_scan_job
        sig = inspect.signature(run_scan_job)
        params = list(sig.parameters.keys())
        assert "job_id" in params, "run_scan_job should accept job_id"

    def test_run_deep_analysis_accepts_analysis_id(self):
        from apps.worker.app.tasks import run_deep_analysis
        sig = inspect.signature(run_deep_analysis)
        params = list(sig.parameters.keys())
        assert "analysis_id" in params, "run_deep_analysis should accept analysis_id"

    def test_generate_export_accepts_export_job_id(self):
        from apps.worker.app.tasks import generate_export
        sig = inspect.signature(generate_export)
        params = list(sig.parameters.keys())
        assert "export_job_id" in params, "generate_export should accept export_job_id"

    def test_reap_stale_jobs_accepts_stale_minutes(self):
        from apps.worker.app.tasks import reap_stale_jobs
        sig = inspect.signature(reap_stale_jobs)
        params = list(sig.parameters.keys())
        assert "stale_minutes" in params, "reap_stale_jobs should accept stale_minutes"


class TestTaskNames:
    """Verify Celery task name attributes are set correctly."""

    def test_run_backtest_task_name(self):
        from apps.worker.app.tasks import run_backtest
        assert run_backtest.name == "backtests.run"

    def test_run_scan_job_task_name(self):
        from apps.worker.app.tasks import run_scan_job
        assert run_scan_job.name == "scans.run_job"

    def test_run_deep_analysis_task_name(self):
        from apps.worker.app.tasks import run_deep_analysis
        assert run_deep_analysis.name == "analysis.deep_symbol"

    def test_generate_export_task_name(self):
        from apps.worker.app.tasks import generate_export
        assert generate_export.name == "exports.generate"

    def test_reap_stale_jobs_task_name(self):
        from apps.worker.app.tasks import reap_stale_jobs
        assert reap_stale_jobs.name == "maintenance.reap_stale_jobs"
