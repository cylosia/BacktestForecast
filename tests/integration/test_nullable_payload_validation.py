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
