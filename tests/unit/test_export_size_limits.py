"""Tests for export size limit constants and enforcement."""
from __future__ import annotations


def test_max_export_bytes_is_10mb():
    from backtestforecast.services.exports import _MAX_EXPORT_BYTES
    assert _MAX_EXPORT_BYTES == 10 * 1024 * 1024


def test_max_csv_trades_limit():
    from backtestforecast.services.exports import _MAX_CSV_TRADES
    assert _MAX_CSV_TRADES == 10_000


def test_max_pdf_pages_limit():
    from backtestforecast.services.exports import _MAX_PDF_PAGES
    assert _MAX_PDF_PAGES == 50


def test_max_csv_equity_points_limit():
    from backtestforecast.services.exports import _MAX_CSV_EQUITY_POINTS
    assert _MAX_CSV_EQUITY_POINTS == 50_000
