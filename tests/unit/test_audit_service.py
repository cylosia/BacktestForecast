"""Tests for AuditService record and record_always.

Imports are deferred inside test methods to avoid Prometheus metric
re-registration conflicts when running all unit tests together.
"""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock


class TestAuditService:
    def _make_service(self):
        from backtestforecast.services.audit import AuditService
        session = MagicMock()
        service = AuditService(session)
        service.repository = MagicMock()
        return service

    def test_record_passes_request_id(self):
        service = self._make_service()
        event = MagicMock()
        event.request_id = "req-123"
        service.repository.add.return_value = (event, True)

        result = service.record(
            event_type="test.event",
            subject_type="test",
            subject_id="sub-1",
            user_id=uuid.uuid4(),
            request_id="req-123",
        )
        assert result is event
        call_args = service.repository.add.call_args
        created_event = call_args[0][0]
        assert created_event.request_id == "req-123"

    def test_record_returns_none_on_dedup(self):
        service = self._make_service()
        event = MagicMock()
        service.repository.add.return_value = (event, False)

        result = service.record(
            event_type="test.event",
            subject_type="test",
            subject_id="sub-1",
        )
        assert result is None

    def test_record_always_passes_request_id(self):
        service = self._make_service()
        event = MagicMock()
        event.request_id = "req-456"
        service.repository.add_always.return_value = (event, True)

        result = service.record_always(
            event_type="test.event",
            subject_type="test",
            subject_id="sub-1",
            request_id="req-456",
        )
        assert result is event
        call_args = service.repository.add_always.call_args
        created_event = call_args[0][0]
        assert created_event.request_id == "req-456"

    def test_record_handles_none_subject_id(self):
        service = self._make_service()
        event = MagicMock()
        service.repository.add.return_value = (event, True)

        result = service.record(
            event_type="test.event",
            subject_type="test",
            subject_id=None,
        )
        assert result is event
        call_args = service.repository.add.call_args
        created_event = call_args[0][0]
        assert created_event.subject_id is None

    def test_record_converts_uuid_subject_id_to_str(self):
        service = self._make_service()
        event = MagicMock()
        service.repository.add.return_value = (event, True)

        uid = uuid.uuid4()
        service.record(
            event_type="test.event",
            subject_type="test",
            subject_id=uid,
        )
        call_args = service.repository.add.call_args
        created_event = call_args[0][0]
        assert created_event.subject_id == str(uid)
