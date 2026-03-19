"""Verify CSV cell sanitization blocks formula injection."""
from backtestforecast.services.exports import ExportService


def test_sanitize_blocks_formula_prefix():
    assert ExportService._sanitize_csv_cell("=cmd|'/C calc'!A0") == "'=cmd|'/C calc'!A0"
    assert ExportService._sanitize_csv_cell("+1+2") == "'+1+2"
    assert ExportService._sanitize_csv_cell("@SUM(A1:A10)") == "'@SUM(A1:A10)"
    assert ExportService._sanitize_csv_cell("|cmd") == "'|cmd"


def test_sanitize_allows_negative_numbers():
    assert ExportService._sanitize_csv_cell("-123.45") == "-123.45"
    assert ExportService._sanitize_csv_cell("-1,234.56") == "-1,234.56"


def test_sanitize_blocks_tab_prefix():
    result = ExportService._sanitize_csv_cell("\t=cmd")
    assert result.startswith("'"), "Tab-prefixed formula should be quoted"


def test_sanitize_passes_normal_strings():
    assert ExportService._sanitize_csv_cell("AAPL") == "AAPL"
    assert ExportService._sanitize_csv_cell("covered_call") == "covered_call"


def test_sanitize_strips_null_bytes():
    assert "\x00" not in str(ExportService._sanitize_csv_cell("test\x00value"))


def test_sanitize_non_string_passthrough():
    assert ExportService._sanitize_csv_cell(42) == 42
    assert ExportService._sanitize_csv_cell(3.14) == 3.14
    assert ExportService._sanitize_csv_cell(None) is None
