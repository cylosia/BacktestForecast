"""Integration tests for GET /v1/forecasts/{ticker}."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from backtestforecast.errors import NotFoundError
from backtestforecast.models import User


def _set_user_plan(session, *, tier: str, subscription_status: str | None = None):
    user = session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()
    user.plan_tier = tier
    user.subscription_status = subscription_status
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


class TestForecastEndpoint:
    def test_happy_path_valid_ticker(
        self, client, auth_headers, db_session, stub_execution
    ):
        """GET /v1/forecasts/{ticker} returns forecast for valid ticker."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        resp = client.get("/v1/forecasts/AAPL", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "forecast" in data
        assert data["forecast"]["symbol"] == "AAPL"
        assert "expected_return_low_pct" in data["forecast"]
        assert "expected_return_high_pct" in data["forecast"]

    def test_404_nonexistent_ticker(
        self, client, auth_headers, db_session, stub_execution, monkeypatch
    ):
        """GET /v1/forecasts/{ticker} returns 404 when symbol has no data."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        from backtestforecast.services.scans import ScanService

        original_build = ScanService.build_forecast

        def mock_build_forecast(self, *, user, symbol, strategy_type, horizon_days):
            if symbol == "NONEXISTENT":
                raise NotFoundError("Symbol not found.")
            return original_build(
                self, user=user, symbol=symbol,
                strategy_type=strategy_type, horizon_days=horizon_days,
            )

        monkeypatch.setattr(ScanService, "build_forecast", mock_build_forecast)

        resp = client.get("/v1/forecasts/NONEXISTENT", headers=auth_headers)
        assert resp.status_code == 404
        assert resp.json()["error"]["code"] == "not_found"

    def test_422_invalid_ticker_format(self, client, auth_headers, db_session):
        """GET /v1/forecasts/{ticker} returns 422 for invalid ticker format."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        # Symbols must match the shared ticker validator; "123" is invalid
        resp = client.get("/v1/forecasts/123", headers=auth_headers)
        assert resp.status_code == 422
        assert resp.json()["error"]["code"] == "validation_error"

    def test_caret_prefixed_ticker_uses_shared_symbol_validator(
        self, client, auth_headers, db_session, monkeypatch
    ):
        """GET /v1/forecasts/{ticker} should accept ^-prefixed symbols like other routes."""
        client.get("/v1/me", headers=auth_headers)
        _set_user_plan(db_session, tier="pro", subscription_status="active")

        from backtestforecast.schemas.forecasts import ForecastEnvelopeResponse
        from backtestforecast.schemas.scans import HistoricalAnalogForecastResponse
        from backtestforecast.services.scans import ScanService

        def mock_build_forecast(self, *, user, symbol, strategy_type, horizon_days):
            assert symbol == "^VIX"
            return ForecastEnvelopeResponse(
                forecast=HistoricalAnalogForecastResponse(
                    symbol="^VIX",
                    strategy_type=strategy_type,
                    as_of_date=date(2026, 3, 26),
                    horizon_days=horizon_days,
                    trading_days_used=20,
                    analog_count=12,
                    analogs_used=12,
                    expected_return_low_pct=Decimal("-5.0"),
                    expected_return_median_pct=Decimal("1.0"),
                    expected_return_high_pct=Decimal("7.5"),
                    positive_outcome_rate_pct=Decimal("58.0"),
                    summary="Synthetic forecast for validation coverage.",
                    disclaimer="Test fixture only.",
                    analog_dates=[],
                    analog_dates_shown=0,
                ),
                expected_move_abs_pct=Decimal("7.5"),
            )

        monkeypatch.setattr(ScanService, "build_forecast", mock_build_forecast)

        resp = client.get("/v1/forecasts/^VIX", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["forecast"]["symbol"] == "^VIX"
