"""Verify PDF export handles the trade limit correctly."""
from __future__ import annotations

import inspect


def test_pdf_trade_limit_constant():
    from backtestforecast.config import Settings
    from backtestforecast.services.exports import ExportService

    assert Settings.model_fields["max_pdf_trades"].default == 100
    source = inspect.getsource(ExportService._build_pdf)
    assert "max_pdf_trades = get_settings().max_pdf_trades" in source
