"""Item 63: Test AuditEventRepository.add_always() with duplicate subject.

Calling add_always() twice with the same event_type, subject_type, subject_id
should NOT raise IntegrityError. The UUID suffix makes each subject_id unique.
"""
from __future__ import annotations

from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from backtestforecast.models import AuditEvent
from backtestforecast.repositories.audit_events import AuditEventRepository


def test_add_always_with_same_subject_does_not_raise(db_session: Session) -> None:
    repo = AuditEventRepository(db_session)

    event1 = AuditEvent(
        event_type="billing.webhook.test_event",
        subject_type="stripe_event",
        subject_id="evt_duplicate_123",
        user_id=None,
        metadata_json={},
    )
    result1, _ = repo.add_always(event1)
    db_session.flush()

    event2 = AuditEvent(
        event_type="billing.webhook.test_event",
        subject_type="stripe_event",
        subject_id="evt_duplicate_123",
        user_id=None,
        metadata_json={},
    )
    result2, _ = repo.add_always(event2)
    db_session.flush()

    assert result1.id is not None
    assert result2.id is not None
    assert result1.id != result2.id

    assert result1.subject_id != result2.subject_id, (
        "UUID suffixes should make subject_id unique across calls"
    )

    rows = db_session.execute(
        select(AuditEvent).where(
            AuditEvent.event_type == "billing.webhook.test_event",
            AuditEvent.subject_type == "stripe_event",
        )
    ).scalars().all()
    assert len(rows) == 2


def test_add_always_null_subject_id_does_not_crash(db_session: Session) -> None:
    """Item 69: Calling add_always twice with NULL subject_id must not crash
    and should log a warning on the second call (unique partial index on NULL)."""
    repo = AuditEventRepository(db_session)

    event1 = AuditEvent(
        event_type="test.null_subject",
        subject_type="test_obj",
        subject_id=None,
        user_id=None,
        metadata_json={},
    )
    result1, _ = repo.add_always(event1)
    db_session.flush()
    assert result1.id is not None

    event2 = AuditEvent(
        event_type="test.null_subject",
        subject_type="test_obj",
        subject_id=None,
        user_id=None,
        metadata_json={},
    )
    result2, _ = repo.add_always(event2)
    db_session.flush()

    assert result2.id is not None


def test_add_always_uuid_suffix_format(db_session: Session) -> None:
    repo = AuditEventRepository(db_session)

    original_subject_id = "my-subject-42"
    event = AuditEvent(
        event_type="test.always_suffix",
        subject_type="test_obj",
        subject_id=original_subject_id,
        user_id=None,
        metadata_json={},
    )
    repo.add_always(event)
    db_session.flush()

    assert event.subject_id is not None
    assert event.subject_id.startswith(f"{original_subject_id}:")
    assert len(event.subject_id) > len(original_subject_id) + 1
