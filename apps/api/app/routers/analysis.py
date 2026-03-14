from __future__ import annotations

import re
from typing import Annotated, Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_request_metadata
from apps.api.app.dispatch import dispatch_celery_task
from backtestforecast.billing.entitlements import ensure_forecasting_access
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db
from backtestforecast.errors import ValidationError
from backtestforecast.models import User
from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService
from backtestforecast.schemas.analysis import (
    AnalysisDetailResponse,
    AnalysisListResponse,
    AnalysisSummaryResponse,
    CreateAnalysisRequest,
)
from backtestforecast.security import get_rate_limiter

_SYMBOL_RE = re.compile(r"^[A-Za-z0-9./^]{1,16}$")

router = APIRouter(prefix="/analysis", tags=["analysis"])
logger = structlog.get_logger("api.analysis")


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
        from backtestforecast.errors import FeatureLockedError
        raise FeatureLockedError("Analysis is temporarily disabled.", required_tier="free")
    ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
    get_rate_limiter().check(
        bucket="analysis:create",
        actor_key=str(user.id),
        limit=settings.analysis_create_rate_limit,
        window_seconds=settings.analysis_rate_limit_window_seconds,
    )

    symbol = payload.symbol.strip().upper()
    if not _SYMBOL_RE.match(symbol):
        raise ValidationError("Symbol must be 1-16 alphanumeric characters (letters, digits, ., /, ^).")

    idempotency_key = payload.idempotency_key

    service = SymbolDeepAnalysisService(
        db,
        market_data_fetcher=None,
        backtest_executor=None,
    )
    analysis = service.create_analysis(user, symbol, idempotency_key=idempotency_key)

    dispatch_celery_task(
        db=db,
        job=analysis,
        task_name="analysis.deep_symbol",
        task_kwargs={"analysis_id": str(analysis.id)},
        queue="research",
        log_event="analysis",
        logger=logger,
        request_id=metadata.request_id,
        traceparent=request.headers.get("traceparent"),
    )

    db.expire_all()
    return _to_summary(analysis)


@router.get("/{analysis_id}", response_model=AnalysisDetailResponse)
def get_analysis(
    analysis_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> AnalysisDetailResponse:
    """Get full analysis results (for polling and display)."""
    get_rate_limiter().check(
        bucket="analysis:read",
        actor_key=str(user.id),
        limit=settings.analysis_create_rate_limit * 5,
        window_seconds=settings.rate_limit_window_seconds,
    )
    ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
    service = SymbolDeepAnalysisService(
        db,
        market_data_fetcher=None,
        backtest_executor=None,
    )
    analysis = service.get_analysis(user, analysis_id)

    summary = _to_summary(analysis)
    if analysis.status == "succeeded":
        return AnalysisDetailResponse(
            **summary.model_dump(),
            regime=analysis.regime_json,
            landscape=analysis.landscape_json,
            top_results=analysis.top_results_json,
            forecast=analysis.forecast_json,
        )
    return AnalysisDetailResponse(**summary.model_dump())


@router.get("/{analysis_id}/status", response_model=AnalysisSummaryResponse)
def get_analysis_status(
    analysis_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> AnalysisSummaryResponse:
    """Lightweight status endpoint for polling."""
    get_rate_limiter().check(
        bucket="analysis:read",
        actor_key=str(user.id),
        limit=settings.analysis_create_rate_limit * 5,
        window_seconds=settings.rate_limit_window_seconds,
    )
    ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
    service = SymbolDeepAnalysisService(
        db,
        market_data_fetcher=None,
        backtest_executor=None,
    )
    analysis = service.get_analysis(user, analysis_id)
    return _to_summary(analysis)


@router.get("", response_model=AnalysisListResponse)
def list_analyses(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: int = Query(default=10, ge=1, le=50),
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    settings: Settings = Depends(get_settings),
) -> AnalysisListResponse:
    """List recent analyses for the current user."""
    get_rate_limiter().check(
        bucket="analysis:read",
        actor_key=str(user.id),
        limit=settings.analysis_create_rate_limit * 5,
        window_seconds=settings.rate_limit_window_seconds,
    )
    ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
    service = SymbolDeepAnalysisService(
        db,
        market_data_fetcher=None,
        backtest_executor=None,
    )
    analyses = service.list_for_user(user, limit=limit, offset=offset)
    total = service.count_for_user(user)
    return {
        "items": [_to_summary(a) for a in analyses],
        "total": total,
        "offset": offset,
        "limit": limit,
    }


def _to_summary(analysis: Any) -> AnalysisSummaryResponse:
    return AnalysisSummaryResponse.model_validate(analysis)
