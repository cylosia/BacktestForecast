from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

from starlette.requests import Request


class _FakeRateLimiter:
    def check(self, **kwargs):
        return None


class _FakeAuditService:
    def __init__(self, session):
        self.session = session

    def record_always(self, **kwargs):
        self.session.audit_events.append(kwargs)


def _request_with_token(token: str) -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/admin/remediation",
        "headers": [(b"authorization", f"Bearer {token}".encode())],
        "client": ("127.0.0.1", 12345),
    }
    return Request(scope)


def test_admin_remediation_can_cancel_active_job(monkeypatch):
    from apps.api.app import main

    job_id = uuid4()
    job = SimpleNamespace(id=job_id, status="running", celery_task_id="task-1", completed_at=None, updated_at=None, error_code=None, error_message=None)
    session = SimpleNamespace(job=job, audit_events=[], commit=lambda: None, get=lambda model, lookup_id: job if lookup_id == job_id else None)

    @contextmanager
    def fake_create_session():
        yield session

    revoked: list[tuple[str | None, str, str]] = []
    published: list[tuple[str, str, str]] = []

    monkeypatch.setattr(main, "get_settings", lambda: SimpleNamespace(admin_token="admin-secret", metrics_token=None))
    monkeypatch.setattr(main, "get_rate_limiter", lambda: _FakeRateLimiter())
    monkeypatch.setattr("backtestforecast.db.session.create_session", fake_create_session)
    monkeypatch.setattr("backtestforecast.services.audit.AuditService", _FakeAuditService)
    monkeypatch.setattr(
        "backtestforecast.services.job_cancellation.revoke_celery_task",
        lambda task_id, *, job_type, job_id: revoked.append((task_id, job_type, str(job_id))),
    )
    monkeypatch.setattr(
        "backtestforecast.services.job_cancellation.publish_cancellation_event",
        lambda *, job_type, job_id, error_code="cancelled_by_support": published.append((job_type, str(job_id), error_code)),
    )

    response = main.admin_remediation(
        _request_with_token("admin-secret"),
        main._AdminRemediationRequest(action="cancel_job", job_type="backtest", job_id=str(job_id)),
    )

    assert response.status_code == 200
    assert job.status == "cancelled"
    assert revoked == [("task-1", "backtest", str(job_id))]
    assert published == [("backtest", str(job_id), "cancelled_by_support")]


def test_admin_remediation_can_dispatch_stripe_cleanup(monkeypatch):
    from apps.api.app import main

    user_id = uuid4()
    session = SimpleNamespace(audit_events=[], commit=lambda: None)

    @contextmanager
    def fake_create_session():
        yield session

    dispatched: list[tuple[str | None, str | None, str, str]] = []

    monkeypatch.setattr(main, "get_settings", lambda: SimpleNamespace(admin_token="admin-secret", metrics_token=None))
    monkeypatch.setattr(main, "get_rate_limiter", lambda: _FakeRateLimiter())
    monkeypatch.setattr("backtestforecast.db.session.create_session", fake_create_session)
    monkeypatch.setattr("backtestforecast.services.audit.AuditService", _FakeAuditService)
    monkeypatch.setattr(
        "apps.api.app.routers.account._dispatch_stripe_cleanup_retry",
        lambda subscription_id, customer_id, user_id_arg, sync_result: dispatched.append(
            (subscription_id, customer_id, str(user_id_arg), sync_result)
        ),
    )

    response = main.admin_remediation(
        _request_with_token("admin-secret"),
        main._AdminRemediationRequest(
            action="dispatch_stripe_cleanup",
            subscription_id="sub_123",
            customer_id="cus_456",
            user_id=str(user_id),
        ),
    )

    assert response.status_code == 200
    assert dispatched == [("sub_123", "cus_456", str(user_id), "support_manual_dispatch")]
