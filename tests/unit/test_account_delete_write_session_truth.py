from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from apps.api.app.routers import account
from backtestforecast.models import User


class _FakeBillingService:
    def __init__(self, db):
        self.db = db

    def cancel_in_flight_jobs(self, _user_id):
        return []

    def close(self):
        return None


class _FakeAuditService:
    recorded_events: list[dict] = []

    def __init__(self, db):
        self.db = db

    def record_always(self, **kwargs):
        self.recorded_events.append(kwargs)
        return kwargs


class _FakeMetric:
    def labels(self, **kwargs):
        return self

    def inc(self):
        return None


class _FakeRateLimiter:
    def check(self, **kwargs):
        return None


class _FakeSession:
    def __init__(self, refreshed_user: User):
        self.refreshed_user = refreshed_user
        self.deleted = None
        self.committed = False
        self.commit_calls = 0

    def get(self, model, user_id):
        assert model is User
        assert user_id == self.refreshed_user.id
        return self.refreshed_user

    def delete(self, user):
        self.deleted = user

    def commit(self):
        self.commit_calls += 1
        self.committed = True

    def rollback(self):
        raise AssertionError("rollback should not be called in this happy-path test")


def test_delete_account_uses_primary_session_user_for_stripe_cleanup(monkeypatch):
    stale_user = User(clerk_user_id="clerk_stale", email="stale@example.com")
    stale_user.id = uuid4()
    stale_user.stripe_subscription_id = None
    stale_user.stripe_customer_id = None
    stale_user.plan_tier = "free"

    refreshed_user = User(clerk_user_id="clerk_fresh", email="fresh@example.com")
    refreshed_user.id = stale_user.id
    refreshed_user.stripe_subscription_id = "sub_live"
    refreshed_user.stripe_customer_id = "cus_live"
    refreshed_user.plan_tier = "pro"

    db = _FakeSession(refreshed_user)
    cleanup_calls: list[tuple[str | None, str | None]] = []

    monkeypatch.setattr("backtestforecast.services.billing.BillingService", _FakeBillingService)
    monkeypatch.setattr(account, "AuditService", _FakeAuditService)
    monkeypatch.setattr(account, "ACCOUNT_DELETIONS_TOTAL", _FakeMetric())
    monkeypatch.setattr(account, "get_rate_limiter", lambda: _FakeRateLimiter())
    monkeypatch.setattr(account, "_cleanup_export_storage", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(account, "_dispatch_stripe_cleanup_retry", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        account,
        "_cleanup_stripe",
        lambda billing, subscription_id, customer_id, user_id: cleanup_calls.append((subscription_id, customer_id)) or "ok",
    )

    account.delete_account(
        user=stale_user,
        metadata=SimpleNamespace(request_id="req-1", ip_address="127.0.0.1"),
        db=db,
        x_confirm_delete="permanently-delete-my-account",
    )

    assert db.deleted is refreshed_user
    assert db.committed is True
    assert cleanup_calls == [("sub_live", "cus_live")]


def test_delete_account_uses_primary_session_truth_under_read_replica_lag(monkeypatch):
    stale_read_replica_user = User(clerk_user_id="clerk_replica", email="replica@example.com")
    stale_read_replica_user.id = uuid4()
    stale_read_replica_user.stripe_subscription_id = None
    stale_read_replica_user.stripe_customer_id = None
    stale_read_replica_user.plan_tier = "free"

    primary_user = User(clerk_user_id="clerk_primary", email="primary@example.com")
    primary_user.id = stale_read_replica_user.id
    primary_user.stripe_subscription_id = "sub_primary"
    primary_user.stripe_customer_id = "cus_primary"
    primary_user.plan_tier = "premium"

    db = _FakeSession(primary_user)
    cleanup_calls: list[tuple[str | None, str | None]] = []

    monkeypatch.setattr("backtestforecast.services.billing.BillingService", _FakeBillingService)
    monkeypatch.setattr(account, "AuditService", _FakeAuditService)
    monkeypatch.setattr(account, "ACCOUNT_DELETIONS_TOTAL", _FakeMetric())
    monkeypatch.setattr(account, "get_rate_limiter", lambda: _FakeRateLimiter())
    monkeypatch.setattr(account, "_cleanup_export_storage", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(account, "_dispatch_stripe_cleanup_retry", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        account,
        "_cleanup_stripe",
        lambda billing, subscription_id, customer_id, user_id: cleanup_calls.append((subscription_id, customer_id)) or "ok",
    )

    account.delete_account(
        user=stale_read_replica_user,
        metadata=SimpleNamespace(request_id="req-replica-lag", ip_address="127.0.0.1"),
        db=db,
        x_confirm_delete="permanently-delete-my-account",
    )

    assert db.deleted is primary_user
    assert cleanup_calls == [("sub_primary", "cus_primary")]


def test_delete_account_records_partial_cleanup_audit_before_retry_dispatch(monkeypatch):
    stale_user = User(clerk_user_id="clerk_partial", email="partial@example.com")
    stale_user.id = uuid4()
    stale_user.stripe_subscription_id = "sub_partial"
    stale_user.stripe_customer_id = "cus_partial"
    stale_user.plan_tier = "pro"

    db = _FakeSession(stale_user)
    dispatched: list[tuple[str | None, str | None, str]] = []
    _FakeAuditService.recorded_events = []

    monkeypatch.setattr("backtestforecast.services.billing.BillingService", _FakeBillingService)
    monkeypatch.setattr(account, "AuditService", _FakeAuditService)
    monkeypatch.setattr(account, "ACCOUNT_DELETIONS_TOTAL", _FakeMetric())
    monkeypatch.setattr(account, "get_rate_limiter", lambda: _FakeRateLimiter())
    monkeypatch.setattr(account, "_cleanup_export_storage", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(account, "_cleanup_stripe", lambda *_args, **_kwargs: "partial")
    monkeypatch.setattr(
        account,
        "_dispatch_stripe_cleanup_retry",
        lambda subscription_id, customer_id, user_id, sync_result: dispatched.append(
            (subscription_id, customer_id, sync_result)
        ),
    )

    account.delete_account(
        user=stale_user,
        metadata=SimpleNamespace(request_id="req-partial", ip_address="127.0.0.1"),
        db=db,
        x_confirm_delete="permanently-delete-my-account",
    )

    event_types = [event["event_type"] for event in _FakeAuditService.recorded_events]
    assert event_types == [
        "account.deleted",
        "account.external_cleanup_started",
        "account.external_cleanup_finished",
        "account.delete_partial_cleanup",
        "account.external_cleanup_retry_dispatched",
    ]
    assert dispatched == [("sub_partial", "cus_partial", "partial")]


class _FailingDeleteCommitSession(_FakeSession):
    def commit(self):
        self.commit_calls += 1
        self.committed = True
        if self.commit_calls == 1:
            raise RuntimeError("delete commit failed")

    def rollback(self):
        return None


def test_delete_account_records_failure_audit_when_delete_commit_fails(monkeypatch):
    user = User(clerk_user_id="clerk_fail", email="fail@example.com")
    user.id = uuid4()
    user.plan_tier = "free"

    db = _FailingDeleteCommitSession(user)
    _FakeAuditService.recorded_events = []

    monkeypatch.setattr("backtestforecast.services.billing.BillingService", _FakeBillingService)
    monkeypatch.setattr(account, "AuditService", _FakeAuditService)
    monkeypatch.setattr(account, "ACCOUNT_DELETIONS_TOTAL", _FakeMetric())
    monkeypatch.setattr(account, "get_rate_limiter", lambda: _FakeRateLimiter())
    monkeypatch.setattr(account, "_cleanup_export_storage", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(account, "_dispatch_stripe_cleanup_retry", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(account, "_cleanup_stripe", lambda *_args, **_kwargs: "ok")

    try:
        account.delete_account(
            user=user,
            metadata=SimpleNamespace(request_id="req-failed-delete", ip_address="127.0.0.1"),
            db=db,
            x_confirm_delete="permanently-delete-my-account",
        )
    except RuntimeError as exc:
        assert "delete commit failed" in str(exc)
    else:
        raise AssertionError("delete_account should re-raise the commit failure")

    event_types = [event["event_type"] for event in _FakeAuditService.recorded_events]
    assert "account.delete_failed" in event_types
