from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from backtestforecast.schemas.backtests import CreateBacktestRunRequest
from backtestforecast.services.backtests import BacktestService


def _request(strategy_type: str = "naked_call", risk_free_rate: Decimal | None = None) -> CreateBacktestRunRequest:
    return CreateBacktestRunRequest(
        symbol="SPY",
        strategy_type=strategy_type,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 3, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
        risk_free_rate=risk_free_rate,
    )


def test_build_user_warnings_includes_naked_option_and_static_rfr(monkeypatch) -> None:
    settings = SimpleNamespace(risk_free_rate=0.045)
    monkeypatch.setattr("backtestforecast.services.backtests.get_settings", lambda: settings)

    service = BacktestService(session=None)  # type: ignore[arg-type]
    warnings = service._build_user_warnings(_request())
    codes = {warning["code"] for warning in warnings}

    assert "naked_option_margin_only" in codes
    assert "configured_static_risk_free_rate" in codes
    static_warning = next(w for w in warnings if w["code"] == "configured_static_risk_free_rate")
    assert "2024-01-02" in static_warning["message"]
    assert static_warning["metadata"]["configured_risk_free_rate"] == 0.045


def test_build_user_warnings_skips_static_rfr_when_request_overrides_rate(monkeypatch) -> None:
    settings = SimpleNamespace(risk_free_rate=0.031)
    monkeypatch.setattr("backtestforecast.services.backtests.get_settings", lambda: settings)

    service = BacktestService(session=None)  # type: ignore[arg-type]
    warnings = service._build_user_warnings(_request(strategy_type="long_call", risk_free_rate=Decimal("0.02")))

    assert warnings == []
