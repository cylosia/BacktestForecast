"""Tests verifying completed_at is set consistently on all failure paths.

Covers:
  - Fix 3: generate_export AppError handler sets completed_at
  - Fix 4: run_scan_job AppError handler sets completed_at
  - Fix 9: _reap_queued_jobs sets updated_at
  - Fix 11: backtests.py error-path UPDATEs include updated_at
"""
from __future__ import annotations

import inspect


class TestFix3ExportAppErrorCompletedAt:
    """Fix 3: generate_export AppError must set completed_at."""

    def test_apperror_handler_sets_completed_at(self):
        from apps.worker.app.tasks import generate_export
        source = inspect.getsource(generate_export)

        apperror_block_start = source.find("except AppError as exc:")
        assert apperror_block_start > 0, "AppError handler not found in generate_export"

        soft_timeout_start = source.find("except SoftTimeLimitExceeded:", apperror_block_start)
        if soft_timeout_start < 0:
            soft_timeout_start = len(source)

        apperror_block = source[apperror_block_start:soft_timeout_start]
        assert "completed_at" in apperror_block, (
            "generate_export AppError handler must set completed_at "
            "for consistent failure-mode timestamps"
        )


class TestFix4ScanAppErrorCompletedAt:
    """Fix 4: run_scan_job AppError must set completed_at."""

    def test_apperror_handler_sets_completed_at(self):
        from apps.worker.app.tasks import run_scan_job
        source = inspect.getsource(run_scan_job)

        apperror_block_start = source.find("except AppError as exc:")
        assert apperror_block_start > 0, "AppError handler not found in run_scan_job"

        soft_timeout_start = source.find("except SoftTimeLimitExceeded:", apperror_block_start)
        if soft_timeout_start < 0:
            soft_timeout_start = len(source)

        apperror_block = source[apperror_block_start:soft_timeout_start]
        assert "completed_at" in apperror_block, (
            "run_scan_job AppError handler must set completed_at "
            "for consistent failure-mode timestamps"
        )


class TestFix9ReapQueuedUpdatedAt:
    """Fix 9: _reap_queued_jobs bulk UPDATE must include updated_at."""

    def test_reap_queued_includes_updated_at(self):
        from apps.worker.app.tasks import _reap_queued_jobs
        source = inspect.getsource(_reap_queued_jobs)
        assert "updated_at" in source, (
            "_reap_queued_jobs must include updated_at in its bulk UPDATE "
            "to keep timestamps accurate"
        )


class TestFix11BacktestErrorPaths:
    """Fix 11: execute_run_by_id error-path UPDATEs include updated_at."""

    def test_all_update_paths_include_updated_at(self):
        from backtestforecast.services.backtests import BacktestService
        source = inspect.getsource(BacktestService.execute_run_by_id)

        import re
        update_blocks = re.findall(
            r'\.values\((.*?)\)',
            source,
            re.DOTALL,
        )
        for i, block in enumerate(update_blocks):
            if 'status=' in block or '"status"' in block:
                assert "updated_at" in block, (
                    f"UPDATE block #{i + 1} sets status but missing updated_at: "
                    f"{block[:100]}..."
                )
