from __future__ import annotations


def test_safe_validate_model_rejects_invalid_payload_without_fabricating_object() -> None:
    from backtestforecast.schemas.analysis import LandscapeCell
    from backtestforecast.services.serialization import safe_validate_model

    result = safe_validate_model(LandscapeCell, {"strategy_type": "long_call", "target_dte": "bad"}, "landscape", default=None)

    assert result is None


def test_safe_validate_model_list_skips_invalid_items() -> None:
    from backtestforecast.schemas.analysis import LandscapeCell
    from backtestforecast.services.serialization import safe_validate_model_list

    results = safe_validate_model_list(
        LandscapeCell,
        [
            {"strategy_type": "long_call", "strategy_label": "Long Call", "target_dte": 30, "config": {}, "score": 1.0},
            {"strategy_type": "long_call", "target_dte": "bad"},
        ],
        "landscape_json",
    )

    assert len(results) == 1
    assert results[0].target_dte == 30


def test_safe_validate_summary_records_integrity_warning() -> None:
    from backtestforecast.services.serialization import safe_validate_summary

    warnings: list[dict[str, str]] = []
    result = safe_validate_summary({"bad": "data"}, field_name="summary_json", response_warnings=warnings)

    assert result.trade_count == 0
    assert any(w["code"] == "stored_payload_invalid" for w in warnings)
    assert any("summary_json" in w["message"] for w in warnings)


def test_safe_validate_json_records_integrity_warning() -> None:
    from backtestforecast.services.serialization import safe_validate_json

    warnings: list[dict[str, str]] = []
    result = safe_validate_json("bad-payload", "request_snapshot_json", default={}, response_warnings=warnings)

    assert result == {}
    assert any(w["code"] == "stored_payload_invalid" for w in warnings)
    assert any("request_snapshot_json" in w["message"] for w in warnings)


def test_scanner_recommendation_nullable_derived_fields_no_longer_require_fabricated_defaults() -> None:
    from uuid import uuid4

    from backtestforecast.schemas.scans import ScannerRecommendationResponse

    response = ScannerRecommendationResponse.model_validate(
        {
            "id": str(uuid4()),
            "rank": 1,
            "score": "1.0",
            "symbol": "AAPL",
            "strategy_type": "long_call",
            "rule_set_name": "baseline",
            "request_snapshot": {},
            "summary": {
                "trade_count": 1,
                "total_commissions": "1",
                "total_net_pnl": "10",
                "starting_equity": "10000",
                "ending_equity": "10010",
            },
            "warnings": [],
            "historical_performance": None,
            "forecast": None,
            "ranking_breakdown": None,
            "trades": [],
            "equity_curve": [],
            "trades_truncated": False,
        }
    )

    assert response.historical_performance is None
    assert response.forecast is None
    assert response.ranking_breakdown is None


def test_analysis_top_result_nullable_fields_do_not_fabricate_empty_summary_or_config() -> None:
    from backtestforecast.schemas.analysis import AnalysisTopResult

    result = AnalysisTopResult.model_validate(
        {
            "rank": 1,
            "strategy_type": "long_call",
            "strategy_label": "Long Call",
            "target_dte": 30,
            "trades": [],
            "equity_curve": [],
            "score": 1.0,
        }
    )

    assert result.config is None
    assert result.summary is None
    assert result.forecast is None
