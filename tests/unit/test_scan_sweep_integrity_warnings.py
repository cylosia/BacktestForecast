from __future__ import annotations

from decimal import Decimal
from uuid import uuid4


def test_scanner_recommendation_response_surfaces_payload_integrity_warnings() -> None:
    from backtestforecast.models import ScannerRecommendation
    from backtestforecast.services.scans import ScanService

    recommendation = ScannerRecommendation(
        id=uuid4(),
        scanner_job_id=uuid4(),
        rank=1,
        score=Decimal("1.25"),
        symbol="AAPL",
        strategy_type="long_call",
        rule_set_name="baseline",
        rule_set_hash="abc123",
        request_snapshot_json="bad-payload",
        summary_json={"bad": "summary"},
        warnings_json=[],
        historical_performance_json={"bad": "payload"},
        forecast_json={"bad": "payload"},
        ranking_features_json={"bad": "payload"},
        trades_json=[{"bad": "trade"}],
        equity_curve_json=[{"bad": "equity"}],
    )

    response = ScanService._to_recommendation_response(recommendation)

    assert response.request_snapshot == {}
    assert response.summary.trade_count == 0
    assert response.historical_performance is None
    assert response.forecast is None
    assert response.ranking_breakdown is None
    assert response.trades == []
    assert response.equity_curve == []
    assert len(response.warnings) >= 1
    assert all(w.code == "stored_payload_invalid" for w in response.warnings)


def test_sweep_result_response_surfaces_payload_integrity_warnings() -> None:
    from backtestforecast.models import SweepResult
    from backtestforecast.services.sweeps import SweepService

    result = SweepResult(
        id=uuid4(),
        sweep_job_id=uuid4(),
        rank=1,
        score=Decimal("0.5"),
        strategy_type="bull_put_spread",
        parameter_snapshot_json="bad-payload",
        summary_json={"bad": "summary"},
        warnings_json=[],
        trades_json=[],
        equity_curve_json=[{"bad": "equity"}],
    )

    response = SweepService._to_result_response(result)

    assert response.parameter_snapshot_json == {}
    assert response.summary.trade_count == 0
    assert response.equity_curve == []
    assert len(response.warnings) >= 1
    assert all(w.code == "stored_payload_invalid" for w in response.warnings)
