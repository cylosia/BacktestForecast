from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

from backtestforecast.billing import events as billing_events
from backtestforecast.services import audit as audit_module


def test_log_billing_event_writes_failed_audits_to_file(monkeypatch, tmp_path) -> None:
    fallback_file = tmp_path / "billing-audit.jsonl"

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
