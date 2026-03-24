from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from datetime import date
from typing import Annotated, Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import ValidationError
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_current_user_readonly, get_request_metadata
from backtestforecast.billing.entitlements import ensure_forecasting_access
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db
from backtestforecast.errors import AppValidationError, FeatureLockedError
from backtestforecast.feature_flags import is_feature_enabled
from backtestforecast.models import User
from backtestforecast.pagination import finalize_cursor_page, parse_cursor_param
from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService
from backtestforecast.schemas.analysis import (
    AnalysisDetailResponse,
    AnalysisForecast,
    AnalysisListResponse,
    AnalysisSummaryResponse,
    AnalysisTopResult,
    CreateAnalysisRequest,
    LandscapeCell,
    RegimeDetail,
)
from backtestforecast.schemas.backtests import SYMBOL_ALLOWED_CHARS
from backtestforecast.schemas.common import RemediationActionsResponse, sanitize_error_message
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.dispatch_recovery import get_dispatch_diagnostic
from backtestforecast.services.remediation_actions import build_job_remediation_actions

router = APIRouter(prefix="/analysis", tags=["analysis"])
logger = structlog.get_logger("api.analysis")


@contextmanager
def _analysis_service(db: Session) -> Generator[SymbolDeepAnalysisService, None, None]:
    svc = SymbolDeepAnalysisService(db, market_data_fetcher=None, backtest_executor=None)
    try:
        yield svc
    finally:
        if hasattr(svc, "close"):
            svc.close()


@router.post("", response_model=AnalysisSummaryResponse, status_code=status.HTTP_202_ACCEPTED)
def create_analysis(
    payload: CreateAnalysisRequest,
    request: Request,
    user: User = Depends(get_current_user),
    metadata=Depends(get_request_metadata),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> AnalysisSummaryResponse:
    """Create and enqueue a single-symbol deep analysis (Pro+ gated)."""
    if not settings.feature_analysis_enabled:
        raise FeatureLockedError("Analysis is temporarily disabled.", required_tier="free")
    if not is_feature_enabled("analysis", user_id=user.id, plan_tier=user.plan_tier):
        raise FeatureLockedError("Analysis is temporarily disabled for this account.", required_tier="free")
    ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
    get_rate_limiter().check(
        bucket="analysis:create",
        actor_key=str(user.id),
        limit=settings.analysis_create_rate_limit,
        window_seconds=settings.analysis_rate_limit_window_seconds,
    )

    symbol = payload.symbol.strip().upper()
    if not SYMBOL_ALLOWED_CHARS.match(symbol):
        raise AppValidationError("Symbol must be 1-16 alphanumeric characters (letters, digits, ., /, ^).")

    idempotency_key = payload.idempotency_key

    with _analysis_service(db) as service:
        analysis = service.create_and_dispatch_analysis(
            user,
            symbol,
            idempotency_key=idempotency_key,
            request_id=metadata.request_id,
            traceparent=request.headers.get("traceparent"),
            dispatch_logger=logger,
        )
        if analysis.status == "failed":
            raise HTTPException(status_code=500, detail={"code": "enqueue_failed", "message": sanitize_error_message(analysis.error_message) or "Unable to dispatch job."})
        return _to_summary(analysis)


@router.get("/{analysis_id}", response_model=AnalysisDetailResponse)
def get_analysis(
    analysis_id: UUID,
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> AnalysisDetailResponse:
    """Get full analysis results (for polling and display)."""
    get_rate_limiter().check(
        bucket="analysis:read",
        actor_key=str(user.id),
        limit=settings.analysis_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with _analysis_service(db) as service:
        analysis = service.get_analysis(user, analysis_id)

        summary = _to_summary(analysis)
        detail_kwargs = summary.model_dump()
        if analysis.status == "succeeded":
            integrity_warnings: list[str] = []
            expected_as_of_date = analysis.trade_date
            top_results = _validate_analysis_list(
                AnalysisTopResult,
                analysis.top_results_json,
                field_name="top_results_json",
                warnings=integrity_warnings,
            )
            top_results = _validate_top_result_forecast_contexts(
                top_results,
                symbol=analysis.symbol,
                as_of_date=expected_as_of_date,
                warnings=integrity_warnings,
            )
            expected_top_result = top_results[0] if top_results else None
            detail_kwargs.update(
                regime=_validate_analysis_regime(analysis.regime_json, integrity_warnings),
                landscape=_validate_analysis_list(
                    LandscapeCell,
                    analysis.landscape_json,
                    field_name="landscape_json",
                    warnings=integrity_warnings,
                ),
                top_results=top_results,
                forecast=_validate_analysis_forecast(
                    analysis.forecast_json,
                    integrity_warnings,
                    expected_symbol=analysis.symbol,
                    expected_strategy_type=expected_top_result.strategy_type if expected_top_result is not None else None,
                    expected_horizon_days=(
                        expected_top_result.max_holding_days or expected_top_result.target_dte
                    )
                    if expected_top_result is not None
                    else None,
                    expected_as_of_date=expected_as_of_date,
                ),
                integrity_warnings=integrity_warnings,
            )
        return AnalysisDetailResponse(**detail_kwargs)


@router.get("/{analysis_id}/status", response_model=AnalysisSummaryResponse)
def get_analysis_status(
    analysis_id: UUID,
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> AnalysisSummaryResponse:
    """Lightweight status endpoint for polling."""
    get_rate_limiter().check(
        bucket="analysis:read",
        actor_key=str(user.id),
        limit=settings.analysis_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with _analysis_service(db) as service:
        analysis = service.get_analysis(user, analysis_id)
        return _to_summary(analysis)


@router.delete("/{analysis_id}", status_code=204)
def delete_analysis(
    analysis_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> None:
    """Delete an analysis. Fails with 409 if the analysis is queued or running."""
    get_rate_limiter().check(
        bucket="analysis:delete",
        actor_key=str(user.id),
        limit=settings.delete_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with _analysis_service(db) as service:
        service.delete_for_user(analysis_id, user.id)


@router.post("/{analysis_id}/cancel", response_model=AnalysisSummaryResponse)
def cancel_analysis(
    analysis_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> AnalysisSummaryResponse:
    get_rate_limiter().check(
        bucket="analysis:delete",
        actor_key=str(user.id),
        limit=settings.delete_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with _analysis_service(db) as service:
        return _to_summary(service.cancel_for_user(analysis_id, user.id))


@router.get("/{analysis_id}/remediation-actions", response_model=RemediationActionsResponse)
def get_analysis_remediation_actions(
    analysis_id: UUID,
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> RemediationActionsResponse:
    get_rate_limiter().check(
        bucket="analysis:read",
        actor_key=str(user.id),
        limit=settings.analysis_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with _analysis_service(db) as service:
        analysis = service.get_analysis(user, analysis_id)
    return build_job_remediation_actions(
        resource_type="analysis",
        resource_id=str(analysis_id),
        status=analysis.status,
        base_path=f"/v1/analysis/{analysis_id}",
    )


@router.get("", response_model=AnalysisListResponse)
def list_analyses(
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_db),
    limit: int = Query(default=10, ge=1, le=50),
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    cursor: Annotated[str | None, Query(max_length=200, description="Opaque cursor from a previous response's next_cursor field. When provided, offset is ignored.")] = None,
    settings: Settings = Depends(get_settings),
) -> AnalysisListResponse:
    """List recent analyses for the current user."""
    get_rate_limiter().check(
        bucket="analysis:read",
        actor_key=str(user.id),
        limit=settings.analysis_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    cursor_before, offset = parse_cursor_param(cursor) if cursor else (None, offset)

    effective_limit = min(limit, 50)
    with _analysis_service(db) as service:
        analyses, total = service.list_for_user(
            user, limit=effective_limit + 1, offset=offset,
            cursor_before=cursor_before,
        )
        page = finalize_cursor_page(analyses, total=total, offset=offset, limit=effective_limit)
        return AnalysisListResponse(
            items=[_to_summary(a) for a in page.items],
            total=page.total,
            offset=page.offset,
            limit=page.limit,
            next_cursor=page.next_cursor,
        )


def _to_summary(analysis: Any) -> AnalysisSummaryResponse:
    summary = AnalysisSummaryResponse.model_validate(analysis)
    diagnostic = get_dispatch_diagnostic(analysis)
    if diagnostic is not None and summary.error_code is None:
        summary.error_code = diagnostic[0]
        summary.error_message = diagnostic[1]
    return summary


def _validate_analysis_regime(regime_json: Any, warnings: list[str]) -> RegimeDetail | None:
    if regime_json is None:
        return None
    try:
        return RegimeDetail.model_validate(regime_json)
    except ValidationError:
        logger.warning("analysis.invalid_regime_json", got_type=type(regime_json).__name__, exc_info=True)
        warnings.append("Stored regime data was corrupted and could not be shown.")
        return None


def _validate_analysis_forecast(
    forecast_json: Any,
    warnings: list[str],
    *,
    expected_symbol: str | None = None,
    expected_strategy_type: str | None = None,
    expected_horizon_days: int | None = None,
    expected_as_of_date: date | None = None,
) -> AnalysisForecast | None:
    required_forecast_keys = {
        "symbol",
        "strategy_type",
        "as_of_date",
        "horizon_days",
        "analog_count",
        "expected_return_low_pct",
        "expected_return_median_pct",
        "expected_return_high_pct",
        "positive_outcome_rate_pct",
        "summary",
        "disclaimer",
    }
    if forecast_json is None:
        return None
    if not isinstance(forecast_json, dict):
        logger.warning("analysis.invalid_forecast_json", got_type=type(forecast_json).__name__)
        warnings.append("Stored forecast data was corrupted and could not be shown.")
        return None
    has_no_results_message = "no_results_message" in forecast_json
    has_full_forecast = required_forecast_keys.issubset(forecast_json)
    if not (has_no_results_message or has_full_forecast):
        logger.warning(
            "analysis.invalid_forecast_json_missing_required_keys",
            item_keys=list(forecast_json.keys()),
        )
        warnings.append("Stored forecast data was corrupted and could not be shown.")
        return None
    try:
        forecast = AnalysisForecast.model_validate(forecast_json)
    except ValidationError:
        logger.warning(
            "analysis.invalid_forecast_json",
            got_type=type(forecast_json).__name__,
            item_keys=list(forecast_json.keys()),
            exc_info=True,
        )
        warnings.append("Stored forecast data was corrupted and could not be shown.")
        return None
    if expected_symbol is not None and forecast.symbol is not None and forecast.symbol != expected_symbol:
        logger.warning(
            "analysis.forecast_context_mismatch",
            field="symbol",
            expected=expected_symbol,
            actual=forecast.symbol,
        )
        warnings.append("Stored forecast data did not match the analyzed symbol and was omitted.")
        return None
    if expected_strategy_type is not None and forecast.strategy_type != expected_strategy_type:
        logger.warning(
            "analysis.forecast_context_mismatch",
            field="strategy_type",
            expected=expected_strategy_type,
            actual=forecast.strategy_type,
        )
        warnings.append("Stored forecast data did not match the analyzed strategy and was omitted.")
        return None
    if expected_horizon_days is not None and forecast.horizon_days is not None and forecast.horizon_days != expected_horizon_days:
        logger.warning(
            "analysis.forecast_context_mismatch",
            field="horizon_days",
            expected=expected_horizon_days,
            actual=forecast.horizon_days,
        )
        warnings.append("Stored forecast data did not match the analyzed horizon and was omitted.")
        return None
    if expected_as_of_date is not None and forecast.as_of_date is not None and forecast.as_of_date != expected_as_of_date:
        logger.warning(
            "analysis.forecast_context_mismatch",
            field="as_of_date",
            expected=expected_as_of_date.isoformat(),
            actual=forecast.as_of_date.isoformat(),
        )
        warnings.append("Stored forecast data did not match the analyzed date and was omitted.")
        return None
    return forecast


def _validate_analysis_list(model_cls: type, payload: Any, *, field_name: str, warnings: list[str]) -> list[Any]:
    if payload is None:
        return []
    if not isinstance(payload, list):
        logger.warning("analysis.invalid_json_collection", field=field_name, got_type=type(payload).__name__)
        warnings.append(f"Stored {field_name.removesuffix('_json').replace('_', ' ')} data was corrupted and could not be shown.")
        return []
    validated: list[Any] = []
    dropped_items = 0
    for item in payload:
        try:
            validated.append(model_cls.model_validate(item))
        except ValidationError:
            dropped_items += 1
            logger.warning(
                "analysis.invalid_json_item_skipped",
                field=field_name,
                model=getattr(model_cls, "__name__", str(model_cls)),
                got_type=type(item).__name__,
                item_keys=list(item.keys()) if isinstance(item, dict) else None,
            )
    if dropped_items:
        label = field_name.removesuffix("_json").replace("_", " ")
        warnings.append(f"{dropped_items} stored {label} item(s) were corrupted and were omitted.")
    return validated


def _validate_top_result_forecast_contexts(
    top_results: list[AnalysisTopResult],
    *,
    symbol: str,
    as_of_date: date | None,
    warnings: list[str],
) -> list[AnalysisTopResult]:
    for result in top_results:
        if result.forecast is None:
            continue
        expected_horizon_days = result.max_holding_days or result.target_dte
        normalized = _validate_analysis_forecast(
            result.forecast.model_dump(mode="json"),
            warnings,
            expected_symbol=symbol,
            expected_strategy_type=result.strategy_type,
            expected_horizon_days=expected_horizon_days,
            expected_as_of_date=as_of_date,
        )
        result.forecast = normalized
    return top_results
