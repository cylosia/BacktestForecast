from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user
from backtestforecast.billing.entitlements import ensure_forecasting_access
from backtestforecast.config import get_settings
from backtestforecast.db.session import get_db
from backtestforecast.models import DailyRecommendation, NightlyPipelineRun, User
from backtestforecast.schemas.analysis import (
    DailyPicksResponse,
    PipelineHistoryResponse,
)
from backtestforecast.security import get_rate_limiter

router = APIRouter(prefix="/daily-picks", tags=["daily-picks"])


@router.get("", response_model=DailyPicksResponse)
def get_latest_daily_picks(
    response: Response,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    trade_date: date | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=50),
) -> dict[str, Any]:
    """Return the latest daily recommendations.

    Pro+ feature gated via ensure_forecasting_access.
    """
    settings = get_settings()
    get_rate_limiter().check(
        bucket="daily_picks:get",
        actor_key=str(user.id),
        limit=settings.daily_picks_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
    response.headers["Cache-Control"] = "private, max-age=300"

    if trade_date is not None:
        run_stmt = (
            select(NightlyPipelineRun)
            .where(
                NightlyPipelineRun.trade_date == trade_date,
                NightlyPipelineRun.status == "succeeded",
            )
            .order_by(desc(NightlyPipelineRun.created_at))
            .limit(1)
        )
    else:
        run_stmt = (
            select(NightlyPipelineRun)
            .where(NightlyPipelineRun.status == "succeeded")
            .order_by(desc(NightlyPipelineRun.created_at))
            .limit(1)
        )

    pipeline_run = db.scalar(run_stmt)
    if pipeline_run is None:
        return {
            "trade_date": trade_date.isoformat() if trade_date else None,
            "pipeline_run_id": None,
            "status": "no_data",
            "items": [],
            "pipeline_stats": None,
        }

    rec_stmt = (
        select(DailyRecommendation)
        .where(DailyRecommendation.pipeline_run_id == pipeline_run.id)
        .order_by(DailyRecommendation.rank)
        .limit(limit)
    )
    recommendations = list(db.scalars(rec_stmt))

    run = pipeline_run
    dur = float(run.duration_seconds) if run.duration_seconds else None
    completed = run.completed_at.isoformat() if run.completed_at else None

    return {
        "trade_date": run.trade_date.isoformat(),
        "pipeline_run_id": str(run.id),
        "status": "ok",
        "pipeline_stats": {
            "symbols_screened": run.symbols_screened,
            "symbols_after_screen": run.symbols_after_screen,
            "pairs_generated": run.pairs_generated,
            "quick_backtests_run": run.quick_backtests_run,
            "full_backtests_run": run.full_backtests_run,
            "recommendations_produced": run.recommendations_produced,
            "duration_seconds": dur,
            "completed_at": completed,
        },
        "items": [
            {
                "rank": rec.rank,
                "score": float(rec.score),
                "symbol": rec.symbol,
                "strategy_type": rec.strategy_type,
                "regime_labels": [label for label in rec.regime_labels.split(",") if label] if rec.regime_labels else [],
                "close_price": float(rec.close_price),
                "target_dte": rec.target_dte,
                "config_snapshot": rec.config_snapshot_json,
                "summary": rec.summary_json,
                "forecast": rec.forecast_json,
            }
            for rec in recommendations
        ],
    }


@router.get("/history", response_model=PipelineHistoryResponse)
def get_pipeline_history(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: int = Query(default=10, ge=1, le=30),
    cursor: str | None = Query(default=None, description="created_at ISO cursor from previous page"),
) -> dict[str, Any]:
    """Return recent pipeline run history (Pro+ gated).

    Pipeline runs are system-wide (not user-scoped) since they represent
    shared nightly scanning results.  Access is gated by forecasting
    entitlement.

    Supports optional cursor-based pagination via the ``cursor`` parameter
    which should be the ``created_at`` ISO timestamp of the last item from
    the previous page.
    """
    from datetime import datetime as _dt

    settings = get_settings()
    get_rate_limiter().check(
        bucket="daily_picks:history",
        actor_key=str(user.id),
        limit=settings.daily_picks_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    ensure_forecasting_access(user.plan_tier, user.subscription_status, user.subscription_current_period_end)

    stmt = select(NightlyPipelineRun).order_by(desc(NightlyPipelineRun.created_at))
    if cursor:
        try:
            cursor_dt = _dt.fromisoformat(cursor)
        except ValueError:
            from backtestforecast.errors import ValidationError as _VE
            raise _VE("Invalid pagination cursor format. Expected an ISO 8601 timestamp.")
        stmt = stmt.where(NightlyPipelineRun.created_at < cursor_dt)
    stmt = stmt.limit(limit)
    runs = list(db.scalars(stmt))

    next_cursor = runs[-1].created_at.isoformat() if runs else None

    return {
        "items": [
            {
                "id": str(r.id),
                "trade_date": r.trade_date.isoformat(),
                "status": r.status,
                "symbols_screened": r.symbols_screened,
                "recommendations_produced": r.recommendations_produced,
                "duration_seconds": (float(r.duration_seconds) if r.duration_seconds else None),
                "completed_at": (r.completed_at.isoformat() if r.completed_at else None),
                "error_message": r.error_message,
            }
            for r in runs
        ],
        "next_cursor": next_cursor,
    }
