from __future__ import annotations

import re
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user
from apps.worker.app.celery_app import celery_app
from backtestforecast.billing.entitlements import ensure_forecasting_access
from backtestforecast.config import get_settings
from backtestforecast.db.session import get_db
from backtestforecast.errors import ValidationError
from backtestforecast.models import User
from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService
from backtestforecast.security import rate_limiter

_SYMBOL_RE = re.compile(r"^[A-Za-z]{1,10}$")


class CreateAnalysisRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=10)
    idempotency_key: str | None = Field(default=None, max_length=80)

router = APIRouter(prefix="/analysis", tags=["analysis"])
settings = get_settings()
logger = structlog.get_logger("api.analysis")


@router.post("", status_code=status.HTTP_202_ACCEPTED)
def create_analysis(
    payload: CreateAnalysisRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Create and enqueue a single-symbol deep analysis (Pro+ gated)."""
    ensure_forecasting_access(user.plan_tier, user.subscription_status)
    rate_limiter.check(
        bucket="analysis:create",
        actor_key=str(user.id),
        limit=settings.analysis_create_rate_limit,
        window_seconds=settings.analysis_rate_limit_window_seconds,
    )

    symbol = payload.symbol.strip().upper()
    if not _SYMBOL_RE.match(symbol):
        raise ValidationError("Symbol must be 1-10 alphabetic characters.")

    idempotency_key = payload.idempotency_key

    service = SymbolDeepAnalysisService(
        db,
        market_data_fetcher=None,
        backtest_executor=None,
    )
    analysis = service.create_analysis(user, symbol, idempotency_key=idempotency_key)

    if analysis.status == "queued":
        try:
            result = celery_app.send_task(
                "analysis.deep_symbol",
                kwargs={"analysis_id": str(analysis.id)},
                queue="research",
            )
            analysis.celery_task_id = result.id
            db.commit()
            logger.info("analysis.enqueued", analysis_id=str(analysis.id), symbol=symbol)
        except Exception:
            logger.exception("analysis.enqueue_failed", analysis_id=str(analysis.id))

    return _to_summary(analysis)


@router.get("/{analysis_id}")
def get_analysis(
    analysis_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Get full analysis results (for polling and display)."""
    service = SymbolDeepAnalysisService(
        db,
        market_data_fetcher=None,
        backtest_executor=None,
    )
    analysis = service.get_analysis(user, analysis_id)

    result: dict[str, Any] = _to_summary(analysis)
    if analysis.status == "succeeded":
        result["regime"] = analysis.regime_json
        result["landscape"] = analysis.landscape_json
        result["top_results"] = analysis.top_results_json
        result["forecast"] = analysis.forecast_json
    return result


@router.get("/{analysis_id}/status")
def get_analysis_status(
    analysis_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Lightweight status endpoint for polling."""
    service = SymbolDeepAnalysisService(
        db,
        market_data_fetcher=None,
        backtest_executor=None,
    )
    analysis = service.get_analysis(user, analysis_id)
    return _to_summary(analysis)


@router.get("")
def list_analyses(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, Any]:
    """List recent analyses for the current user."""
    service = SymbolDeepAnalysisService(
        db,
        market_data_fetcher=None,
        backtest_executor=None,
    )
    analyses = service.list_for_user(user, limit=limit)
    return {
        "items": [_to_summary(a) for a in analyses],
    }


def _to_summary(analysis: Any) -> dict[str, Any]:
    a = analysis
    return {
        "id": str(a.id),
        "symbol": a.symbol,
        "status": a.status,
        "stage": a.stage,
        "close_price": float(a.close_price) if a.close_price else None,
        "strategies_tested": a.strategies_tested,
        "configs_tested": a.configs_tested,
        "top_results_count": a.top_results_count,
        "duration_seconds": (float(a.duration_seconds) if a.duration_seconds else None),
        "error_message": a.error_message,
        "created_at": (a.created_at.isoformat() if a.created_at else None),
        "completed_at": (a.completed_at.isoformat() if a.completed_at else None),
    }
