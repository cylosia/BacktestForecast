from __future__ import annotations

from pathlib import Path


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_orphaned_stripe_cleanup_alert_exists() -> None:
    source = _read("ops/prometheus_alerts.yml")
    assert "alert: OrphanedStripeCleanupFailures" in source
    assert "external_cleanup_failures_total" in source
    assert "stripe_cleanup_retry" in source


def test_support_runbooks_exist_for_stuck_jobs_and_orphaned_billing_cleanup() -> None:
    stuck_jobs = _read("docs/support-stuck-jobs.md")
    orphaned_billing = _read("docs/support-orphaned-billing-cleanup.md")

    assert "Support Runbook: Stuck Jobs" in stuck_jobs
    assert "cancel endpoint" in stuck_jobs.lower()
    assert "repair_stranded_jobs.py" in stuck_jobs

    assert "Support Runbook: Orphaned Billing Cleanup" in orphaned_billing
    assert "account.delete_partial_cleanup" in orphaned_billing
    assert "maintenance.cleanup_stripe_orphan" in orphaned_billing


def test_monitoring_docs_cover_external_cleanup_failures() -> None:
    source = _read("docs/monitoring-alerting.md")
    assert "external_cleanup_failures_total" in source
    assert "30s, 60s, 120s, 240s, 480s" in source


def test_runbook_links_to_support_docs() -> None:
    source = _read("docs/RUNBOOK.md")
    assert "docs/support-stuck-jobs.md" in source
    assert "docs/support-orphaned-billing-cleanup.md" in source
    assert "<<<<<<<" not in source
