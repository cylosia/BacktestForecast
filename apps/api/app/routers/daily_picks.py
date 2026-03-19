from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Generator

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user
from backtestforecast.billing.entitlements import ensure_forecasting_access
from backtestforecast.config import get_settings
from backtestforecast.db.session import get_db
from backtestforecast.errors import ValidationError
from backtestforecast.models import User
from backtestforecast.schemas.analysis import (
    DailyPicksResponse,
    PipelineHistoryResponse,
)
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.daily_picks import DailyPicksService
from backtestforecast.utils.dates import market_date_today

router = APIRouter(prefix="/daily-picks", tags=["daily-picks"])


@contextmanager
def _daily_picks_service(db: Session) -> Generator[DailyPicksService, None, None]:
    svc = DailyPicksService(db)
    try:
        yield svc
    finally:
        if hasattr(svc, "close"):
            svc.close()


@router.get("", response_model=DailyPicksResponse)
def get_latest_daily_picks(
    response: Response,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    trade_date: date | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=50),
    offset: int = Query(default=0, ge=0, le=10000),
) -> DailyPicksResponse:
    """Return the latest daily recommendations.

    Pro+ feature gated via ensure_forecasting_access.
    """
    settings = get_settings()
    if not settings.feature_daily_picks_enabled:
        from backtestforecast.errors import FeatureLockedError
        raise FeatureLockedError("Daily picks are temporarily disabled.", required_tier="free")
    get_rate_limiter().check(
        bucket="daily_picks:get",
        actor_key=str(user.id),
        limit=settings.daily_picks_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
    response.headers["Cache-Control"] = "private, max-age=300"

    if trade_date is not None:
        today = market_date_today()
        if trade_date > today:
            raise ValidationError("trade_date cannot be in the future.")
        if trade_date < today - timedelta(days=5 * 365):
            raise ValidationError("trade_date cannot be more than 5 years in the past.")

    with _daily_picks_service(db) as service:
        return DailyPicksResponse.model_validate(
            service.get_latest_picks(trade_date=trade_date, limit=limit, offset=offset)
        )


@router.get("/history", response_model=PipelineHistoryResponse)
def get_pipeline_history(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: int = Query(default=10, ge=1, le=30),
    cursor: str | None = Query(default=None, max_length=50, description="created_at ISO cursor from previous page"),
) -> PipelineHistoryResponse:
    """Return recent pipeline run history (Pro+ gated).

    Supports optional cursor-based pagination via the ``cursor`` parameter
    which should be the ``created_at`` ISO timestamp of the last item from
    the previous page.
    """
    settings = get_settings()
    if not settings.feature_daily_picks_enabled:
        from backtestforecast.errors import FeatureLockedError
        raise FeatureLockedError("Daily picks are temporarily disabled.", required_tier="free")
    get_rate_limiter().check(
        bucket="daily_picks:history",
        actor_key=str(user.id),
        limit=settings.daily_picks_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)

    if cursor is not None:
        try:
            parsed = datetime.fromisoformat(cursor)
        except (ValueError, TypeError) as exc:
            raise ValidationError("cursor must be a valid ISO 8601 timestamp.") from exc
        if parsed.tzinfo is None:
            raise ValidationError("cursor must include timezone information (e.g. +00:00 or Z).")

    with _daily_picks_service(db) as service:
        return PipelineHistoryResponse.model_validate(service.get_history(limit=limit, cursor=cursor))
