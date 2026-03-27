"""Tests for daily recommendations cleanup task (audit items 17-19)."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.orm import Session

from backtestforecast.models import DailyRecommendation, NightlyPipelineRun

pytestmark = pytest.mark.postgres


@pytest.fixture()
def session(postgres_db_session: Session) -> Session:
    return postgres_db_session


def _make_pipeline_run(session: Session, *, days_ago: int) -> NightlyPipelineRun:
    run = NightlyPipelineRun(
        trade_date=datetime.now(UTC).date() - timedelta(days=days_ago),
        status="succeeded",
        created_at=datetime.now(UTC) - timedelta(days=days_ago),
    )
    session.add(run)
    session.flush()
    return run


def _make_rec(session: Session, run: NightlyPipelineRun, rank: int) -> DailyRecommendation:
    rec = DailyRecommendation(
        pipeline_run_id=run.id,
        trade_date=run.trade_date,
        rank=rank,
        score=0.5,
        symbol="AAPL",
        strategy_type="long_call",
        close_price=150.0,
        target_dte=30,
        created_at=run.created_at,
    )
    session.add(rec)
    session.flush()
    return rec


def test_old_recommendations_are_deleted(session: Session) -> None:
    old_run = _make_pipeline_run(session, days_ago=120)
    new_run = _make_pipeline_run(session, days_ago=5)
    old_rec = _make_rec(session, old_run, rank=1)
    new_rec = _make_rec(session, new_run, rank=1)
    session.commit()

    from datetime import datetime as dt

    from sqlalchemy import delete, select

    cutoff = dt.now(UTC) - timedelta(days=90)
    batch_ids = list(session.scalars(
        select(DailyRecommendation.id)
        .where(DailyRecommendation.created_at < cutoff)
        .limit(2000)
    ))
    assert len(batch_ids) == 1
    assert batch_ids[0] == old_rec.id

    session.execute(
        delete(DailyRecommendation).where(DailyRecommendation.id.in_(batch_ids))
    )
    session.commit()

    remaining = list(session.scalars(select(DailyRecommendation.id)))
    assert len(remaining) == 1
    assert remaining[0] == new_rec.id


def test_task_is_registered_in_beat_schedule() -> None:
    from apps.worker.app.celery_app import celery_app

    schedule = celery_app.conf.beat_schedule
    assert "cleanup-daily-recommendations-weekly" in schedule
    entry = schedule["cleanup-daily-recommendations-weekly"]
    assert entry["task"] == "maintenance.cleanup_daily_recommendations"


def test_task_is_routed_to_maintenance_queue() -> None:
    from apps.worker.app.celery_app import celery_app

    routes = celery_app.conf.task_routes
    assert routes.get("maintenance.cleanup_daily_recommendations") == {"queue": "maintenance"}
