"""Verify PDF export handles the trade limit correctly."""
from __future__ import annotations


def test_pdf_trade_limit_constant():
    from backtestforecast.services.exports import _MAX_PDF_TRADES
    assert _MAX_PDF_TRADES == 100
