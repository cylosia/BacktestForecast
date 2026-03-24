from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from backtestforecast.backtests.run_warnings import build_user_warnings
from backtestforecast.schemas.backtests import CreateBacktestRunRequest


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


def test_build_user_warnings_includes_naked_option_and_historical_treasury_rate(monkeypatch) -> None:
    settings = SimpleNamespace(risk_free_rate=0.045)
    monkeypatch.setattr("backtestforecast.backtests.run_warnings.get_settings", lambda: settings)

    warnings = build_user_warnings(
        _request(),
        resolved_risk_free_rate=0.041,
        risk_free_rate_source="massive_treasury",
    )
    codes = {warning["code"] for warning in warnings}

    assert "naked_option_margin_only" in codes
    assert "historical_treasury_risk_free_rate" in codes
    treasury_warning = next(w for w in warnings if w["code"] == "historical_treasury_risk_free_rate")
    assert "2024-01-02" in treasury_warning["message"]
    assert treasury_warning["metadata"]["configured_risk_free_rate"] == 0.045
    assert treasury_warning["metadata"]["resolved_risk_free_rate"] == 0.041


def test_build_user_warnings_uses_configured_fallback_message_when_massive_unavailable(monkeypatch) -> None:
    settings = SimpleNamespace(risk_free_rate=0.045)
    monkeypatch.setattr("backtestforecast.backtests.run_warnings.get_settings", lambda: settings)

    warnings = build_user_warnings(
        _request(strategy_type="long_call"),
        resolved_risk_free_rate=0.045,
        risk_free_rate_source="configured_fallback",
    )

    assert [warning["code"] for warning in warnings] == ["configured_fallback_risk_free_rate"]
    assert "Massive Treasury yields were unavailable" in warnings[0]["message"]


def test_build_user_warnings_skips_static_rfr_when_request_overrides_rate(monkeypatch) -> None:
    settings = SimpleNamespace(risk_free_rate=0.031)
    monkeypatch.setattr("backtestforecast.backtests.run_warnings.get_settings", lambda: settings)

    warnings = build_user_warnings(_request(strategy_type="long_call", risk_free_rate=Decimal("0.02")))

    assert warnings == []


def test_build_user_warnings_do_not_claim_static_configured_rate(monkeypatch) -> None:
    settings = SimpleNamespace(risk_free_rate=0.045)
    monkeypatch.setattr("backtestforecast.backtests.run_warnings.get_settings", lambda: settings)

    warnings = build_user_warnings(
        _request(),
        resolved_risk_free_rate=0.041,
        risk_free_rate_source="massive_treasury",
    )

    messages = " ".join(warning["message"] for warning in warnings)
    assert "static configured risk-free rate" not in messages.lower()
