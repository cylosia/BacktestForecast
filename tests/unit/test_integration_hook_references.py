from __future__ import annotations

from pathlib import Path


def test_live_integration_hooks_have_call_sites() -> None:
    root = Path(__file__).resolve().parents[2]
    service_source = (root / "src/backtestforecast/market_data/service.py").read_text(encoding="utf-8")

    assert "def set_ex_dividend_dates(" in service_source
    assert "option_gateway.set_ex_dividend_dates(ex_dividend_dates)" in service_source
