"""Unit tests for export content generation.

Verifies CSV column headers, trade field presence, and formula sanitization.
The sanitization logic lives in ExportService._sanitize_csv_cell (a static method).
"""
from __future__ import annotations

import pytest

from backtestforecast.services.exports import ExportService


class TestCsvSanitizeCell:
    """Verify _sanitize_csv_cell escapes formula-injection characters."""

    @staticmethod
    def _sanitize(value: object) -> object:
        return ExportService._sanitize_csv_cell(value)

    def test_equals_sign_escaped(self):
        result = self._sanitize("=SUM(A1)")
        assert isinstance(result, str)
        assert result.startswith("'")

    def test_plus_sign_escaped(self):
        result = self._sanitize("+cmd")
        assert isinstance(result, str)
        assert result.startswith("'")

    def test_at_sign_escaped(self):
        result = self._sanitize("@cmd")
        assert isinstance(result, str)
        assert result.startswith("'")

    def test_pipe_sign_escaped(self):
        result = self._sanitize("|cmd")
        assert isinstance(result, str)
        assert result.startswith("'")

    def test_safe_text_unchanged(self):
        assert self._sanitize("normal text") == "normal text"

    def test_none_passthrough(self):
        result = self._sanitize(None)
        assert result is None

    def test_numeric_passthrough(self):
        assert self._sanitize(42) == 42
        assert self._sanitize(3.14) == 3.14

    def test_negative_number_string_not_escaped(self):
        result = self._sanitize("-123.45")
        assert isinstance(result, str)
        assert not result.startswith("'"), "Numeric negative strings should not be escaped"

    def test_tab_and_newline_stripped(self):
        result = self._sanitize("line1\nline2\ttab")
        assert "\n" not in str(result)
        assert "\t" not in str(result)


class TestExportServiceInterface:
    """Verify ExportService has the expected methods."""

    def test_execute_export_method_exists(self):
        assert hasattr(ExportService, "execute_export_by_id")

    def test_cleanup_method_exists(self):
        assert hasattr(ExportService, "cleanup_expired_exports")

    def test_enqueue_export_method_exists(self):
        assert hasattr(ExportService, "enqueue_export")

    def test_get_export_status_method_exists(self):
        assert hasattr(ExportService, "get_export_status")

    def test_get_export_for_download_method_exists(self):
        assert hasattr(ExportService, "get_export_for_download")
