"""Test that record_always preserves ip_hash from the original event.

Regression test for the bug where add_always constructed a new AuditEvent
without copying ip_hash, causing all repeatable events (export.downloaded,
backtest.viewed) to lose IP attribution.
"""
from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from backtestforecast.models import AuditEvent, User
from backtestforecast.services.audit import AuditService

pytestmark = pytest.mark.filterwarnings("ignore::UserWarning")


@pytest.fixture()
def audit(db_session: Session) -> AuditService:
    return AuditService(db_session)


def _create_user(db_session: Session) -> User:
    user = User(clerk_user_id=f"audit-ip-{uuid4()}", email="audit-ip@test.com")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


def test_record_always_preserves_ip_hash(audit: AuditService, db_session: Session) -> None:
    user_id = _create_user(db_session).id
    audit.record_always(
        event_type="export.downloaded",
        subject_type="export_job",
        subject_id="job-123",
        user_id=user_id,
        ip_address="198.51.100.42",
    )
    db_session.commit()

    stmt = select(AuditEvent).where(AuditEvent.event_type == "export.downloaded")
    events = list(db_session.execute(stmt).scalars().all())
    assert len(events) == 1
    assert events[0].ip_hash is not None, "ip_hash must be set when ip_address is provided"
    assert len(events[0].ip_hash) > 0


def test_record_always_ip_hash_none_when_no_ip(audit: AuditService, db_session: Session) -> None:
    user_id = _create_user(db_session).id
    audit.record_always(
        event_type="export.downloaded",
        subject_type="export_job",
        subject_id="job-456",
        user_id=user_id,
    )
    db_session.commit()

    stmt = select(AuditEvent).where(AuditEvent.event_type == "export.downloaded")
    events = list(db_session.execute(stmt).scalars().all())
    assert len(events) == 1
    assert events[0].ip_hash is None


def test_record_always_multiple_calls_all_preserve_ip_hash(
    audit: AuditService, db_session: Session,
) -> None:
    user_id = _create_user(db_session).id
    for _ in range(3):
        audit.record_always(
            event_type="export.downloaded",
            subject_type="export_job",
            subject_id="job-789",
            user_id=user_id,
            ip_address="203.0.113.10",
        )
    db_session.commit()

    stmt = select(AuditEvent).where(AuditEvent.event_type == "export.downloaded")
    events = list(db_session.execute(stmt).scalars().all())
    assert len(events) == 3
    for event in events:
        assert event.ip_hash is not None, "Every record_always event should preserve ip_hash"
