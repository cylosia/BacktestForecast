from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user
from backtestforecast.billing.entitlements import ensure_forecasting_access
from backtestforecast.db.session import get_db
from backtestforecast.models import DailyRecommendation, NightlyPipelineRun, User

router = APIRouter(prefix="/daily-picks", tags=["daily-picks"])


@router.get("")
def get_latest_daily_picks(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    trade_date: date | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=50),
) -> dict[str, Any]:
    """Return the latest daily recommendations.

    Pro+ feature gated via ensure_forecasting_access.
    """
    ensure_forecasting_access(user.plan_tier, user.subscription_status)

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
                "regime_labels": (rec.regime_labels.split(",") if rec.regime_labels else []),
                "close_price": float(rec.close_price),
                "target_dte": rec.target_dte,
                "config_snapshot": rec.config_snapshot_json,
                "summary": rec.summary_json,
                "forecast": rec.forecast_json,
            }
            for rec in recommendations
        ],
    }


@router.get("/history")
def get_pipeline_history(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: int = Query(default=10, ge=1, le=30),
) -> dict[str, Any]:
    """Return recent pipeline run history (Pro+ gated)."""
    ensure_forecasting_access(user.plan_tier, user.subscription_status)

    stmt = select(NightlyPipelineRun).order_by(desc(NightlyPipelineRun.created_at)).limit(limit)
    runs = list(db.scalars(stmt))

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
    }
