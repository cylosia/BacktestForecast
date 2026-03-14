"""Item 74: Test CSV injection with leading whitespace.

Verifies that _sanitize_csv_cell correctly catches dangerous characters
even when preceded by leading whitespace (e.g., "  =cmd|...").
"""
from __future__ import annotations

import pytest

from backtestforecast.services.exports import ExportService


class TestCsvSanitizationLeadingWhitespace:
    @staticmethod
    def _sanitize(value: object) -> object:
        return ExportService._sanitize_csv_cell(value)

    def test_leading_space_before_equals(self):
        result = self._sanitize("  =cmd|'/C calc'!A0")
        assert isinstance(result, str)
        stripped = result.strip()
        assert not stripped.startswith("="), (
            f"Expected dangerous '=' to be neutralized, got: {result!r}"
        )

    def test_leading_space_before_plus(self):
        result = self._sanitize("  +cmd|'/C calc'!A0")
        stripped = result.strip()
        assert not stripped.startswith("+"), (
            f"Expected dangerous '+' to be neutralized, got: {result!r}"
        )

    def test_leading_space_before_minus(self):
        result = self._sanitize("  -1+1")
        stripped = result.strip()
        assert not stripped.startswith("-"), (
            f"Expected dangerous '-' to be neutralized, got: {result!r}"
        )

    def test_leading_space_before_at(self):
        result = self._sanitize("  @SUM(A1:A10)")
        stripped = result.strip()
        assert not stripped.startswith("@"), (
            f"Expected dangerous '@' to be neutralized, got: {result!r}"
        )

    def test_leading_space_before_pipe(self):
        result = self._sanitize("  |command")
        stripped = result.strip()
        assert not stripped.startswith("|"), (
            f"Expected dangerous '|' to be neutralized, got: {result!r}"
        )

    def test_no_leading_whitespace_still_sanitized(self):
        result = self._sanitize("=1+2")
        assert isinstance(result, str)
        assert result.startswith("'")

    def test_safe_string_unchanged(self):
        result = self._sanitize("hello world")
        assert result == "hello world"

    def test_non_string_passthrough(self):
        assert self._sanitize(42) == 42
        assert self._sanitize(3.14) == 3.14
        assert self._sanitize(None) is None

    def test_tabs_and_newlines_stripped(self):
        result = self._sanitize("line1\nline2\ttab\rreturn")
        assert "\n" not in str(result)
        assert "\t" not in str(result)
        assert "\r" not in str(result)
