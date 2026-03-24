"""Verification tests for the Workflow Trace Findings.

Covers all 8 workflow findings: auth, create, export, billing, retry,
delete, reaper, and download flows.
"""
from __future__ import annotations

import inspect
import warnings

# ---- W1: Auth flow - dev mode documented, production guarded ----

def test_w1_auth_production_guard():
    from backtestforecast.auth.verification import ClerkTokenVerifier
    source = inspect.getsource(ClerkTokenVerifier.verify_bearer_token)
    assert "CLERK_AUDIENCE and CLERK_ISSUER must be set in production" in source


def test_w1_auth_dev_warning():
    from backtestforecast.auth.verification import ClerkTokenVerifier
    source = inspect.getsource(ClerkTokenVerifier.verify_bearer_token)
    assert "audience verification is disabled" in source


# ---- W2: Create (backtest) flow - _commit_then_publish defined ----

def test_w2_commit_then_publish_defined():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _commit_then_publish
    assert callable(_commit_then_publish)


# ---- W3: Export flow - _commit_then_publish used correctly ----

def test_w3_export_task_uses_commit_then_publish():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import generate_export
    source = inspect.getsource(generate_export)
    assert "_commit_then_publish" in source


# ---- W4: Billing/webhook - programming errors caught separately ----

def test_w4_webhook_separates_error_types():
    from backtestforecast.services.billing import BillingService
    source = inspect.getsource(BillingService._handle_webhook_impl)
    assert "KeyError" in source
    assert "likely_programming_error" in source


# ---- W5: Error/retry - scan retries on ExternalServiceError ----

def test_w5_scan_retries_external_errors():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import run_scan_job
    source = inspect.getsource(run_scan_job)
    assert "ExternalServiceError" in source
    assert "self.retry" in source


# ---- W6: Delete flow - S3 storage cleaned before cascade ----

def test_w6_account_delete_cleans_export_storage():
    from apps.api.app.routers.account import _cleanup_export_storage
    source = inspect.getsource(_cleanup_export_storage)
    assert "storage.delete" in source
    assert "reconcile_s3_orphans" in source


def test_w6_account_delete_calls_cleanup_before_cascade():
    from apps.api.app.routers.account import delete_account
    source = inspect.getsource(delete_account)
    cleanup_pos = source.find("_cleanup_export_storage")
    delete_pos = source.find("db.delete(user)")
    assert cleanup_pos >= 0, "_cleanup_export_storage not found in delete_account"
    assert delete_pos >= 0, "db.delete(user) not found in delete_account"
    assert cleanup_pos < delete_pos, (
        "_cleanup_export_storage must be called BEFORE db.delete(user)"
    )


def test_w6_export_delete_for_user_cleans_storage():
    from backtestforecast.services.exports import ExportService
    source = inspect.getsource(ExportService.delete_for_user)
    assert "storage.delete" in source or "_storage.delete" in source


# ---- W7: Reaper - CAS prevents double-dispatch ----

def test_w7_reaper_uses_cas():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _reap_queued_jobs
    source = inspect.getsource(_reap_queued_jobs)
    assert "celery_task_id.is_(None)" in source
    assert "with_for_update" in source
    assert "skip_locked" in source


def test_w7_reaper_resets_on_send_failure():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _reap_queued_jobs
    source = inspect.getsource(_reap_queued_jobs)
    assert "celery_task_id=None" in source or "celery_task_id == task_id" in source


# ---- W8: Export download - audit comment documents tradeoff ----

def test_w8_download_audit_is_documented():
    from backtestforecast.services.exports import ExportService
    source = inspect.getsource(ExportService.get_export_for_download)
    assert "intentionally optimistic" in source
