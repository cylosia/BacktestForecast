from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from backtestforecast.config import get_settings
from backtestforecast.scans.ranking import HistoricalObservation, hash_payload
from backtestforecast.schemas.backtests import EquityCurvePointResponse, RsiRule, TradeJsonResponse
from backtestforecast.schemas.scans import (
    HistoricalPerformanceResponse,
    ScannerJobResponse,
    ScannerRecommendationResponse,
)
from backtestforecast.services.serialization import (
    safe_validate_json,
    safe_validate_list,
    safe_validate_model,
    safe_validate_summary,
    safe_validate_warning_list,
)


def historical_metric_or_none(summary: dict[str, Any], field: str) -> float | None:
    try:
        value = float(summary.get(field, 0.0))
    except (TypeError, ValueError):
        return None
    return value if value.is_finite() else None


def historical_observation_from_summary(
    *,
    completed_at,
    summary: dict[str, Any],
) -> HistoricalObservation | None:
    if completed_at is None:
        return None
    win_rate = historical_metric_or_none(summary, "win_rate")
    total_roi_pct = historical_metric_or_none(summary, "total_roi_pct")
    max_drawdown_pct = historical_metric_or_none(summary, "max_drawdown_pct")
    if None in (win_rate, total_roi_pct, max_drawdown_pct):
        return None
    return HistoricalObservation(
        completed_at=completed_at,
        win_rate=win_rate,
        total_roi_pct=total_roi_pct,
        max_drawdown_pct=max_drawdown_pct,
    )


def get_fallback_entry_rules() -> list[RsiRule]:
    threshold = get_settings().fallback_entry_rule_rsi_threshold
    return [RsiRule(type="rsi", operator="lte", threshold=Decimal(str(threshold)), period=14)]


@dataclass(slots=True)
class RankedCandidate:
    sort_key: tuple[float, str, str, str]
    candidate: dict[str, Any] = field(compare=False)

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, RankedCandidate):
            return NotImplemented
        return self.sort_key > other.sort_key


def request_hash(payload) -> str:
    base_payload = payload.model_dump(mode="json")
    base_payload.pop("name", None)
    base_payload.pop("idempotency_key", None)
    return hash_payload(base_payload)


def scanner_job_response(job) -> ScannerJobResponse:
    warnings = safe_validate_warning_list(job.warnings_json)
    return ScannerJobResponse(
        id=job.id,
        name=job.name,
        status=job.status,
        mode=job.mode,
        plan_tier_snapshot=job.plan_tier_snapshot,
        job_kind=job.job_kind,
        candidate_count=job.candidate_count,
        evaluated_candidate_count=job.evaluated_candidate_count,
        recommendation_count=job.recommendation_count,
        refresh_daily=job.refresh_daily,
        refresh_priority=job.refresh_priority,
        ranking_version=job.ranking_version,
        engine_version=job.engine_version,
        warnings=warnings,
        error_code=job.error_code,
        error_message=job.error_message,
        idempotency_key=job.idempotency_key,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
    )


def scanner_recommendation_response(recommendation) -> ScannerRecommendationResponse:
    warnings = safe_validate_warning_list(recommendation.warnings_json)
    trades = safe_validate_list(
        TradeJsonResponse,
        recommendation.trades_json,
        "trades",
        response_warnings=warnings,
    )
    equity_curve = safe_validate_list(
        EquityCurvePointResponse,
        recommendation.equity_curve_json,
        "equity_curve",
        response_warnings=warnings,
    )
    persisted_trade_count = max(
        int((recommendation.ranking_features_json or {}).get("trade_count") or 0),
        int((recommendation.summary_json or {}).get("trade_count") or 0),
        len(recommendation.trades_json or []),
    )
    serialized_trade_count = max(
        int((recommendation.ranking_features_json or {}).get("serialized_trade_count") or 0),
        len(recommendation.trades_json or []),
    )
    persisted_equity_point_count = max(
        int((recommendation.ranking_features_json or {}).get("equity_point_count") or 0),
        len(recommendation.equity_curve_json or []),
    )
    serialized_equity_point_count = max(
        int((recommendation.ranking_features_json or {}).get("serialized_equity_point_count") or 0),
        len(recommendation.equity_curve_json or []),
    )
    return ScannerRecommendationResponse(
        id=recommendation.id,
        rank=recommendation.rank,
        score=recommendation.score,
        symbol=recommendation.symbol,
        strategy_type=recommendation.strategy_type,
        rule_set_name=recommendation.rule_set_name,
        request_snapshot=safe_validate_json(
            recommendation.request_snapshot_json,
            "request_snapshot_json",
            default={},
            response_warnings=warnings,
        ),
        summary=safe_validate_summary(
            recommendation.summary_json,
            field_name="summary_json",
            response_warnings=warnings,
        ),
        warnings=warnings,
        historical_performance=safe_validate_model(
            HistoricalPerformanceResponse,
            recommendation.historical_performance_json,
            "historical_performance_json",
            default=None,
            response_warnings=warnings,
        ),
        forecast=safe_validate_model(
            recommendation.forecast_response_model,
            recommendation.forecast_json,
            "forecast_json",
            default=None,
            response_warnings=warnings,
        ) if hasattr(recommendation, "forecast_response_model") else None,
        ranking_breakdown=safe_validate_model(
            recommendation.ranking_response_model,
            recommendation.ranking_breakdown_json,
            "ranking_breakdown_json",
            default=None,
            response_warnings=warnings,
        ) if hasattr(recommendation, "ranking_response_model") else None,
        trades=trades,
        equity_curve=equity_curve,
        trades_truncated=persisted_trade_count > serialized_trade_count,
        trade_items_omitted=max(persisted_trade_count - serialized_trade_count, 0),
        equity_curve_truncated=persisted_equity_point_count > serialized_equity_point_count,
        equity_curve_points_omitted=max(persisted_equity_point_count - serialized_equity_point_count, 0),
    )
