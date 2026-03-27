"""Test: pipeline duplicate-run race condition.

Verifies that the nightly pipeline rejects concurrent runs for the same
trade_date, preventing duplicate work.
"""
from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from backtestforecast.models import NightlyPipelineRun

pytestmark = pytest.mark.postgres

@pytest.fixture()
def db_session(postgres_db_session: Session) -> Session:
    return postgres_db_session


def test_duplicate_pipeline_run_same_date_rejected(db_session):
    """If a succeeded pipeline run already exists for today, a second insert
    with status='succeeded' should be rejected by the unique partial index."""
    today = date.today()
    first = NightlyPipelineRun(
        id=uuid4(),
        trade_date=today,
        status="succeeded",
        symbols_screened=100,
        recommendations_produced=5,
        created_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )
    db_session.add(first)
    db_session.commit()

    second = NightlyPipelineRun(
        id=uuid4(),
        trade_date=today,
        status="succeeded",
        symbols_screened=50,
        recommendations_produced=3,
        created_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )
    db_session.add(second)

    from sqlalchemy import select
    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError):
        db_session.commit()

    db_session.rollback()
    existing = db_session.scalars(
        select(NightlyPipelineRun).where(
            NightlyPipelineRun.trade_date == today,
            NightlyPipelineRun.status == "succeeded",
        )
    ).all()
    assert len(existing) == 1


def test_pipeline_run_different_dates_allowed(db_session):
    """Pipeline runs on different dates should both be allowed."""
    yesterday = date.today() - timedelta(days=1)
    today = date.today()

    r1 = NightlyPipelineRun(
        id=uuid4(),
        trade_date=yesterday,
        status="succeeded",
        symbols_screened=50,
        recommendations_produced=3,
        created_at=datetime.now(UTC),
    )
    r2 = NightlyPipelineRun(
        id=uuid4(),
        trade_date=today,
        status="running",
        symbols_screened=0,
        recommendations_produced=0,
        created_at=datetime.now(UTC),
    )
    db_session.add_all([r1, r2])
    db_session.commit()

    from sqlalchemy import select

    runs = db_session.scalars(select(NightlyPipelineRun)).all()
    assert len(runs) == 2
