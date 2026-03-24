"""Maintenance task re-exports.

This module exists to make ``celery_app.conf.include`` enumerate
maintenance tasks separately from domain tasks.  The actual task
implementations still live in ``tasks.py`` until each one is migrated
here.

Migration plan:
  1. Move one task at a time from tasks.py to this module.
  2. Verify with ``celery inspect registered`` that the task is still
     discoverable after each move.
  3. Once all maintenance.* tasks live here, remove the re-exports.
"""
from apps.worker.app.tasks import (  # noqa: F401
    cleanup_audit_events,
    cleanup_daily_recommendations,
    cleanup_outbox,
    cleanup_stripe_orphan,
    expire_old_exports,
    ping,
    poll_outbox,
    reap_stale_jobs,
    reconcile_s3_orphans,
    reconcile_subscriptions,
    refresh_market_holidays,
    refresh_prioritized_scans,
)
