"""Audit assertion tests for export download and deduplication behavior."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from backtestforecast.models import AuditEvent
from backtestforecast.services.audit import AuditService


@pytest.fixture()
def audit(db_session: Session) -> AuditService:
    return AuditService(db_session)


def test_record_always_creates_multiple_events(audit: AuditService, db_session: Session) -> None:
    """record_always creates a new event each time; subject_id gets UUID suffix."""
    user_id = uuid4()
    audit.record_always(
        event_type="export.downloaded",
        subject_type="export_job",
        subject_id="some-uuid",
        user_id=user_id,
    )
    audit.record_always(
        event_type="export.downloaded",
        subject_type="export_job",
        subject_id="some-uuid",
        user_id=user_id,
    )
    db_session.commit()

    stmt = select(AuditEvent).where(AuditEvent.event_type == "export.downloaded")
    events = list(db_session.execute(stmt).scalars().all())
    assert len(events) == 2
    subject_ids = [e.subject_id for e in events]
    assert subject_ids[0] != subject_ids[1]
    assert all(sid.startswith("some-uuid:") for sid in subject_ids)


def test_record_deduplicates_same_event(audit: AuditService, db_session: Session) -> None:
    """record() deduplicates identical events; only one row is stored."""
    user_id = uuid4()
    audit.record(
        event_type="export.created",
        subject_type="export_job",
        subject_id="some-uuid",
        user_id=user_id,
    )
    audit.record(
        event_type="export.created",
        subject_type="export_job",
        subject_id="some-uuid",
        user_id=user_id,
    )
    db_session.commit()

    stmt = select(AuditEvent).where(AuditEvent.event_type == "export.created")
    events = list(db_session.execute(stmt).scalars().all())
    assert len(events) == 1


def test_record_always_vs_record_independence(audit: AuditService, db_session: Session) -> None:
    """record() and record_always() are independent; total count is correct."""
    user_id = uuid4()
    audit.record(
        event_type="billing.sync",
        subject_type="billing",
        subject_id="sync-1",
        user_id=user_id,
    )
    audit.record_always(
        event_type="export.downloaded",
        subject_type="export_job",
        subject_id="job-1",
        user_id=user_id,
    )
    audit.record_always(
        event_type="export.downloaded",
        subject_type="export_job",
        subject_id="job-1",
        user_id=user_id,
    )
    db_session.commit()

    stmt = select(AuditEvent)
    events = list(db_session.execute(stmt).scalars().all())
    assert len(events) == 3
