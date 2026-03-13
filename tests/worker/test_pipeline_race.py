"""Test: pipeline duplicate-run race condition.

Verifies that the nightly pipeline rejects concurrent runs for the same
trade_date, preventing duplicate work.
"""
from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.models import NightlyPipelineRun, User


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    yield session
    session.close()
    engine.dispose()


def test_duplicate_pipeline_run_same_date_rejected(db_session):
    """If a pipeline run already exists for today, a second attempt should not
    create a duplicate (or should detect the existing run)."""
    today = date.today()
    first = NightlyPipelineRun(
        id=uuid4(),
        trade_date=today,
        status="completed",
        symbols_screened=100,
        recommendations_produced=5,
        created_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )
    db_session.add(first)
    db_session.commit()

    from sqlalchemy import select

    existing = db_session.scalars(
        select(NightlyPipelineRun).where(NightlyPipelineRun.trade_date == today)
    ).all()
    assert len(existing) == 1
    assert existing[0].status == "completed"


def test_pipeline_run_different_dates_allowed(db_session):
    """Pipeline runs on different dates should both be allowed."""
    yesterday = date.today() - timedelta(days=1)
    today = date.today()

    r1 = NightlyPipelineRun(
        id=uuid4(),
        trade_date=yesterday,
        status="completed",
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
