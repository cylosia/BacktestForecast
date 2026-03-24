from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _mock_celery_module(monkeypatch):
    import sys
    import types

    mock_celery = MagicMock()
    mock_celery.task.side_effect = lambda *args, **kwargs: (lambda fn: fn)
    mock_module = types.ModuleType("apps.worker.app.celery_app")
    mock_module.celery_app = mock_celery
    monkeypatch.setitem(sys.modules, "apps.worker.app.celery_app", mock_module)
    return mock_celery


def test_billing_service_reconcile_signature_matches_implementation() -> None:
    from backtestforecast.services.billing import BillingService
    from backtestforecast.services.billing_components import ReconciliationService

    service_sig = BillingService.reconcile_subscriptions.__annotations__
    component_sig = ReconciliationService.reconcile_subscriptions.__annotations__

    assert "grace_hours" in BillingService.reconcile_subscriptions.__code__.co_varnames
    assert "dry_run" in BillingService.reconcile_subscriptions.__code__.co_varnames
    assert "grace_hours" in ReconciliationService.reconcile_subscriptions.__code__.co_varnames
    assert "dry_run" in ReconciliationService.reconcile_subscriptions.__code__.co_varnames
    assert service_sig["return"] == list[dict[str, object]] or "list" in str(service_sig["return"]).lower()
    assert component_sig["return"] == list[dict[str, object]] or "list" in str(component_sig["return"]).lower()


def test_reconcile_subscriptions_task_passes_grace_hours_and_counts_actions(monkeypatch) -> None:
    from apps.worker.app import tasks as tasks_module

    class _FakeBillingService:
        def __init__(self, _session) -> None:
            self.calls: list[tuple[int, bool]] = []

        def reconcile_subscriptions(self, *, grace_hours: int = 48, dry_run: bool = False):
            self.calls.append((grace_hours, dry_run))
            return [{"user_id": "u1"}, {"user_id": "u2"}]

    fake_service = _FakeBillingService(None)

    class _SessionContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    monkeypatch.setattr("backtestforecast.config.get_settings", lambda: SimpleNamespace(active_renewal_grace_hours=72))
    monkeypatch.setattr("backtestforecast.services.billing.BillingService", lambda session: fake_service)
    monkeypatch.setattr(tasks_module, "create_worker_session", lambda: _SessionContext())

    task = tasks_module.reconcile_subscriptions
    result = task.run() if hasattr(task, "run") else task(None)

    assert result == {"reconciled": 2}
    assert fake_service.calls == [(72, False)]
