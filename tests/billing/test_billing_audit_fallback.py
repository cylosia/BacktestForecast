from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from backtestforecast.billing import events as billing_events
from backtestforecast.services import audit as audit_module


def _workspace_fallback_file() -> Path:
    path = Path(".pytest-billing-audit-fallback") / f"{uuid4()}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def test_log_billing_event_writes_failed_audits_to_file(monkeypatch) -> None:
    fallback_file = _workspace_fallback_file()
    try:
        class _FailingAuditService:
            def __init__(self, session) -> None:
                self.session = session

            def record_always(self, **kwargs) -> None:
                raise RuntimeError("db down")

        monkeypatch.setattr(billing_events, "_BILLING_AUDIT_FALLBACK_FILE", fallback_file)
        monkeypatch.setattr(
            billing_events,
            "get_settings",
            lambda: SimpleNamespace(redis_cache_url=None, redis_url=None),
        )
        monkeypatch.setattr(audit_module, "AuditService", _FailingAuditService)

        billing_events.log_billing_event(
            user_id=uuid4(),
            event_type="subscription.updated",
            subscription_id="sub_123",
            old_state={"status": "trialing"},
            new_state={"status": "active"},
            source="webhook",
            request_id="req_123",
            session=object(),
        )

        payloads = [json.loads(line) for line in fallback_file.read_text(encoding="utf-8").splitlines()]
        assert len(payloads) == 1
        assert payloads[0]["event_type"] == "subscription.updated"
        assert payloads[0]["subscription_id"] == "sub_123"
    finally:
        fallback_file.unlink(missing_ok=True)


def test_drain_deferred_billing_audits_replays_file_payloads(monkeypatch) -> None:
    fallback_file = _workspace_fallback_file()
    try:
        fallback_file.write_text(
            json.dumps(
                {
                    "event_type": "subscription.updated",
                    "user_id": str(uuid4()),
                    "subscription_id": "sub_456",
                    "request_id": "req_456",
                    "source": "webhook",
                    "old_state": {"status": "past_due"},
                    "new_state": {"status": "active"},
                    "recorded_at": "2026-03-22T00:00:00+00:00",
                }
            ) + "\n",
            encoding="utf-8",
        )

        recorded: list[dict[str, object]] = []

        class _RecordingAuditService:
            def __init__(self, session) -> None:
                self.session = session

            def record_always(self, **kwargs) -> None:
                recorded.append(kwargs)

        class _Session:
            def flush(self) -> None:
                return None

            def commit(self) -> None:
                return None

            def rollback(self) -> None:
                return None

        monkeypatch.setattr(billing_events, "_BILLING_AUDIT_FALLBACK_FILE", fallback_file)
        monkeypatch.setattr(
            billing_events,
            "get_settings",
            lambda: SimpleNamespace(redis_cache_url=None, redis_url=None),
        )
        monkeypatch.setattr(audit_module, "AuditService", _RecordingAuditService)

        result = billing_events.drain_deferred_billing_audits(_Session(), batch_size=10)

        assert result["drained"] == 1
        assert result["failed"] == 0
        assert recorded[0]["event_type"] == "billing.subscription.updated"
        assert recorded[0]["metadata"]["replayed_from_fallback"] is True
        assert not fallback_file.exists()
    finally:
        fallback_file.unlink(missing_ok=True)


def test_worker_registers_billing_audit_drain_task() -> None:
    from pathlib import Path

    source = Path("apps/worker/app/celery_app.py").read_text(encoding="utf-8")
    assert "maintenance.drain_billing_audit_fallback" in source
    assert '"drain-billing-audit-fallback"' in source
