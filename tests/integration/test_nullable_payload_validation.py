from __future__ import annotations

from datetime import UTC, datetime

from backtestforecast.models import SymbolAnalysis, User


def test_analysis_detail_skips_malformed_json_rows_instead_of_fabricating_defaults(
    client,
    auth_headers,
    db_session,
):
    client.get("/v1/me", headers=auth_headers)
    user = db_session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()

    analysis = SymbolAnalysis(
        user_id=user.id,
        symbol="AAPL",
        status="succeeded",
        stage="forecast",
        regime_json="bad-regime",
        landscape_json=[{"strategy_type": "long_call", "target_dte": "not-an-int"}],
        top_results_json=[{"summary": "bad-summary", "trades": "bad-trades", "equity_curve": {}}],
        forecast_json=["not-a-dict"],
        strategies_tested=3,
        configs_tested=9,
        top_results_count=1,
        created_at=datetime.now(UTC),
    )
    db_session.add(analysis)
    db_session.commit()

    response = client.get(f"/v1/analysis/{analysis.id}", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["regime"] is None
    assert body["landscape"] == []
    assert body["top_results"] == []
    assert body["forecast"] is None
    assert "integrity_warnings" in body
    assert any("regime" in warning.lower() for warning in body["integrity_warnings"])
    assert any("landscape" in warning.lower() for warning in body["integrity_warnings"])
    assert any("top results" in warning.lower() for warning in body["integrity_warnings"])
    assert any("forecast" in warning.lower() for warning in body["integrity_warnings"])


def test_analysis_detail_omits_partial_forecast_payloads_that_fail_canonical_shape(
    client,
    auth_headers,
    db_session,
):
    client.get("/v1/me", headers=auth_headers)
    user = db_session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()

    analysis = SymbolAnalysis(
        user_id=user.id,
        symbol="MSFT",
        status="succeeded",
        stage="forecast",
        regime_json={"regimes": ["bullish"], "close_price": 100.0},
        landscape_json=[],
        top_results_json=[],
        forecast_json={
            "expected_return_median_pct": 5.0,
            "analog_count": 12,
        },
        strategies_tested=1,
        configs_tested=1,
        top_results_count=0,
        created_at=datetime.now(UTC),
    )
    db_session.add(analysis)
    db_session.commit()

    response = client.get(f"/v1/analysis/{analysis.id}", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["forecast"] is None
    assert any("forecast" in warning.lower() for warning in body["integrity_warnings"])


def test_analysis_detail_omits_forecast_payloads_with_context_mismatch(
    client,
    auth_headers,
    db_session,
):
    client.get("/v1/me", headers=auth_headers)
    user = db_session.query(User).filter(User.clerk_user_id == "clerk_test_user").one()

    analysis = SymbolAnalysis(
        user_id=user.id,
        symbol="AAPL",
        status="succeeded",
        stage="forecast",
        regime_json={"regimes": ["bullish"], "close_price": 100.0},
        landscape_json=[],
        top_results_json=[
            {
                "rank": 1,
                "strategy_type": "long_call",
                "target_dte": 30,
                "summary": {
                    "trade_count": 10,
                    "decided_trades": 9,
                    "win_rate": "60",
                    "total_roi_pct": "12",
                    "average_win_amount": "100",
                    "average_loss_amount": "-50",
                    "average_holding_period_days": "10",
                    "average_dte_at_open": "30",
                    "max_drawdown_pct": "5",
                    "total_commissions": "10",
                    "total_net_pnl": "500",
                    "starting_equity": "10000",
                    "ending_equity": "10500",
                    "expectancy": "55",
                    "max_consecutive_wins": 3,
                    "max_consecutive_losses": 1,
                },
                "trades": [],
                "equity_curve": [],
                "forecast": {
                    "symbol": "MSFT",
                    "strategy_type": "long_call",
                    "as_of_date": "2026-03-24",
                    "horizon_days": 30,
                    "analog_count": 10,
                    "expected_return_low_pct": -5,
                    "expected_return_median_pct": 4,
                    "expected_return_high_pct": 12,
                    "positive_outcome_rate_pct": 62,
                    "summary": "Mismatched symbol forecast",
                    "disclaimer": "Probabilistic only.",
                },
                "score": 1.2,
            },
        ],
        forecast_json={
            "symbol": "MSFT",
            "as_of_date": "2026-03-24",
            "horizon_days": 30,
            "analog_count": 12,
            "expected_return_low_pct": -6,
            "expected_return_median_pct": 3,
            "expected_return_high_pct": 10,
            "positive_outcome_rate_pct": 60,
            "summary": "Mismatched top-level forecast",
            "disclaimer": "Probabilistic only.",
        },
        strategies_tested=1,
        configs_tested=1,
        top_results_count=1,
        created_at=datetime.now(UTC),
    )
    db_session.add(analysis)
    db_session.commit()

    response = client.get(f"/v1/analysis/{analysis.id}", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["forecast"] is None
    assert body["top_results"][0]["forecast"] is None
    assert any("forecast" in warning.lower() for warning in body["integrity_warnings"])
