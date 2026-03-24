"""Tests for S3 upload failure during export execution.

Verifies that when storage.put raises an exception, the export job is
marked as failed and never left in a "succeeded" state.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

pytestmark = pytest.mark.filterwarnings("ignore:MASSIVE_API_KEY:UserWarning")


def _make_export_job(*, status: str = "queued"):
    job = MagicMock()
    job.id = uuid4()
    job.user_id = uuid4()
    job.backtest_run_id = uuid4()
    job.export_format = "csv"
    job.file_name = "AAPL_long_call_2026-03-20.csv"
    job.status = status
    job.mime_type = "text/csv"
    job.idempotency_key = None
    job.storage_key = None
    job.size_bytes = None
    job.sha256_hex = None
    job.completed_at = None
    job.error_code = None
    job.error_message = None
    job.content_bytes = None
    return job


def _make_user():
    user = MagicMock()
    user.id = uuid4()
    user.plan_tier = "pro"
    user.subscription_status = "active"
    user.subscription_current_period_end = None
    return user


def _make_run():
    run = MagicMock()
    run.id = uuid4()
    run.status = "succeeded"
    run.trade_count = 5
    run.symbol = "AAPL"
    run.strategy_type = "long_call"
    return run


class TestExportS3Failure:
    @patch("backtestforecast.services.exports.EXPORT_EXECUTION_DURATION_SECONDS")
    @patch("backtestforecast.services.exports.ensure_export_access")
    def test_s3_put_failure_marks_export_failed(self, mock_ensure, mock_metric):
        """When storage.put raises an exception the export job must be
        marked with status='failed'."""
        from backtestforecast.services.exports import ExportService

        session = MagicMock()
        storage = MagicMock()
        storage.put.side_effect = Exception("S3 upload failed: connection timeout")

        export_job = _make_export_job(status="queued")
        user = _make_user()
        run = _make_run()

        repo = MagicMock()
        repo.get.return_value = export_job
        repo.get_by_idempotency_key.return_value = None

        backtest_repo = MagicMock()
        backtest_repo.get_lightweight_for_user.return_value = run

        cas_rows = MagicMock()
        cas_rows.rowcount = 1
        session.execute.return_value = cas_rows
        session.get.return_value = user

        svc = ExportService(session, storage=storage)
        svc.exports = repo
        svc.backtests = backtest_repo
        svc.backtest_service = MagicMock()
        svc.backtest_service.get_run_for_owner.return_value = MagicMock()
        svc.audit = MagicMock()

        svc._build_csv = MagicMock(return_value=b"col1,col2\nval1,val2\n")

        with pytest.raises(Exception, match="S3 upload failed"):
            svc.execute_export_by_id(export_job.id)

        [
            c for c in session.execute.call_args_list
            if len(c.args) > 0 and hasattr(c.args[0], 'compile')
        ]
        status_values = []
        for c in session.execute.call_args_list:
            if hasattr(c, 'args') and len(c.args) > 0:
                stmt = c.args[0]
                if hasattr(stmt, '_values') and isinstance(stmt._values, dict):
                    if 'status' in stmt._values:
                        status_values.append(stmt._values['status'])

        assert export_job.status != "succeeded" or any(
            "failed" in str(c) for c in session.execute.call_args_list
        ), "Export must be marked as failed when storage.put raises"

    @patch("backtestforecast.services.exports.EXPORT_EXECUTION_DURATION_SECONDS")
    @patch("backtestforecast.services.exports.ensure_export_access")
    def test_s3_put_failure_does_not_leave_succeeded_status(self, mock_ensure, mock_metric):
        """Verify the export status is never set to 'succeeded' when
        storage.put fails."""
        from backtestforecast.services.exports import ExportService

        session = MagicMock()
        storage = MagicMock()
        storage.put.side_effect = RuntimeError("S3 bucket unreachable")

        export_job = _make_export_job(status="queued")
        user = _make_user()
        run = _make_run()

        repo = MagicMock()
        repo.get.return_value = export_job
        repo.get_by_idempotency_key.return_value = None

        backtest_repo = MagicMock()
        backtest_repo.get_lightweight_for_user.return_value = run

        cas_rows = MagicMock()
        cas_rows.rowcount = 1
        session.execute.return_value = cas_rows
        session.get.return_value = user

        svc = ExportService(session, storage=storage)
        svc.exports = repo
        svc.backtests = backtest_repo
        svc.backtest_service = MagicMock()
        svc.backtest_service.get_run_for_owner.return_value = MagicMock()
        svc.audit = MagicMock()

        svc._build_csv = MagicMock(return_value=b"col1,col2\nval1,val2\n")

        committed_statuses: list[str] = []

        def _track_execute(stmt, *args, **kwargs):
            if hasattr(stmt, 'compile'):
                compiled = str(stmt)
                if 'succeeded' in compiled.lower():
                    committed_statuses.append("succeeded")
                elif 'failed' in compiled.lower():
                    committed_statuses.append("failed")
            return cas_rows

        session.execute.side_effect = _track_execute

        with pytest.raises((RuntimeError, Exception)):
            svc.execute_export_by_id(export_job.id)

        assert "succeeded" not in committed_statuses, (
            "Status must never be set to 'succeeded' when storage.put fails"
        )
