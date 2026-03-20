from __future__ import annotations

from contextlib import contextmanager
from typing import Annotated, Any, Generator
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_request_metadata
from apps.api.app.dispatch import dispatch_celery_task
from backtestforecast.billing.entitlements import ensure_forecasting_access
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db
from backtestforecast.errors import AppValidationError, FeatureLockedError
from backtestforecast.models import User
from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService
from backtestforecast.schemas.analysis import (
    AnalysisDetailResponse,
    AnalysisListResponse,
    AnalysisSummaryResponse,
    CreateAnalysisRequest,
)
from backtestforecast.schemas.backtests import SYMBOL_ALLOWED_CHARS
from backtestforecast.schemas.common import sanitize_error_message
from backtestforecast.security import get_rate_limiter

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

        db.refresh(analysis)
        if analysis.status == "failed":
            raise HTTPException(status_code=500, detail={"code": "enqueue_failed", "message": sanitize_error_message(analysis.error_message) or "Unable to dispatch job."})
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
        limit=settings.analysis_read_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with _analysis_service(db) as service:
        analysis = service.get_analysis(user, analysis_id)

        summary = _to_summary(analysis)
        detail_kwargs = summary.model_dump()
        if analysis.status == "succeeded":
            detail_kwargs.update(
                regime=analysis.regime_json,
                landscape=analysis.landscape_json,
                top_results=analysis.top_results_json,
                forecast=analysis.forecast_json,
            )
        return AnalysisDetailResponse(**detail_kwargs)


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


@router.get("", response_model=AnalysisListResponse)
def list_analyses(
    user: User = Depends(get_current_user),
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
    from backtestforecast.utils import decode_cursor, encode_cursor

    cursor_before = None
    if cursor:
        cursor_before = decode_cursor(cursor)
        if cursor_before is None:
            from backtestforecast.errors import ValidationError
            raise ValidationError("Invalid pagination cursor.")
        offset = 0

    effective_limit = min(limit, 50)
    with _analysis_service(db) as service:
        analyses = service.list_for_user(
            user, limit=effective_limit + 1, offset=offset,
            cursor_before=cursor_before,
        )
        has_next = len(analyses) > effective_limit
        if has_next:
            analyses = analyses[:effective_limit]
        total = service.count_for_user(user)
        next_cursor = encode_cursor(analyses[-1].created_at, analyses[-1].id) if has_next and analyses else None
        return AnalysisListResponse(
            items=[_to_summary(a) for a in analyses],
            total=total,
            offset=offset,
            limit=effective_limit,
            next_cursor=next_cursor,
        )


def _to_summary(analysis: Any) -> AnalysisSummaryResponse:
    return AnalysisSummaryResponse.model_validate(analysis)
