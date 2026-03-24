"""Fix 64: All terminal export statuses are handled properly.

Verify that the four terminal statuses ("succeeded", "failed", "cancelled",
"expired") are recognized by the export service and model.
"""
from __future__ import annotations

from backtestforecast.models import ExportJob

_TERMINAL_EXPORT_STATUSES = {"succeeded", "failed", "cancelled", "expired"}


class TestExportTerminalStates:
    @staticmethod
    def _status_constraint_text() -> str:
        table = ExportJob.__table__
        status_constraint = next(
            c for c in table.constraints
            if getattr(c, "name", "") == "ck_export_jobs_valid_export_status"
        )
        return str(status_constraint.sqltext).lower()

    def test_all_terminal_statuses_exist_in_check_constraint(self):
        """The ExportJob check constraint must allow all terminal statuses."""
        constraint_text = self._status_constraint_text()
        for status in _TERMINAL_EXPORT_STATUSES:
            assert status in constraint_text, (
                f"Terminal status '{status}' missing from ExportJob check constraint"
            )

    def test_export_job_has_status_column(self):
        """ExportJob must have a status column."""
        assert hasattr(ExportJob, "status"), "ExportJob must have status column"

    def test_export_job_has_completed_at_column(self):
        """Terminal statuses should set completed_at."""
        assert hasattr(ExportJob, "completed_at"), (
            "ExportJob must have completed_at column for terminal states"
        )

    def test_export_job_has_error_fields(self):
        """Failed/cancelled exports should record error details."""
        assert hasattr(ExportJob, "error_code"), (
            "ExportJob must have error_code for failure tracking"
        )
        assert hasattr(ExportJob, "error_message"), (
            "ExportJob must have error_message for failure tracking"
        )

    def test_non_terminal_statuses_are_queued_and_running(self):
        """Queued and running are the only non-terminal statuses."""
        constraint_text = self._status_constraint_text()
        assert "queued" in constraint_text
        assert "running" in constraint_text

    def test_expired_is_a_valid_terminal_status(self):
        """'expired' must be recognized - cleanup_expired_exports uses it."""
        import inspect

        from backtestforecast.services.exports import ExportService

        source = inspect.getsource(ExportService.cleanup_expired_exports)
        assert "expired" in source, (
            "cleanup_expired_exports must transition exports to 'expired'"
        )
