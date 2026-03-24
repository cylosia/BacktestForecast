"""Extended CSV sanitization tests for edge cases."""
from __future__ import annotations

from backtestforecast.services.exports import ExportService


def test_sanitize_leading_zero_number():
    assert ExportService._sanitize_csv_cell("007") == "007"


def test_sanitize_negative_number():
    assert ExportService._sanitize_csv_cell("-42.5") == "-42.5"


def test_sanitize_formula_injection_equals():
    result = ExportService._sanitize_csv_cell("=CMD('malicious')")
    assert result.startswith("'")


def test_sanitize_formula_injection_plus():
    result = ExportService._sanitize_csv_cell("+1+cmd|'/C calc'!A0")
    assert result.startswith("'")


def test_sanitize_formula_injection_at():
    result = ExportService._sanitize_csv_cell("@SUM(A1:A2)")
    assert result.startswith("'")


def test_sanitize_formula_injection_pipe():
    result = ExportService._sanitize_csv_cell("|cmd")
    assert result.startswith("'")


def test_sanitize_non_numeric_dash():
    result = ExportService._sanitize_csv_cell("-cmd('x')")
    assert result.startswith("'")


def test_sanitize_tab_replacement():
    result = ExportService._sanitize_csv_cell("hello\tworld")
    assert "\t" not in str(result)


def test_sanitize_null_byte_removal():
    result = ExportService._sanitize_csv_cell("hello\x00world")
    assert "\x00" not in str(result)


def test_sanitize_non_string_passthrough():
    assert ExportService._sanitize_csv_cell(42) == 42
    assert ExportService._sanitize_csv_cell(3.14) == 3.14
    assert ExportService._sanitize_csv_cell(None) is None


def test_sanitize_empty_string():
    assert ExportService._sanitize_csv_cell("") == ""


def test_sanitize_normal_text():
    assert ExportService._sanitize_csv_cell("AAPL covered_call") == "AAPL covered_call"
