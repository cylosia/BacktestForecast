# Support Runbook: Orphaned Billing Cleanup

## When to use this
- An account was deleted, but Stripe cleanup failed or was only partially completed.
- Alerts based on `external_cleanup_failures_total` are firing.
- Logs show `account.stripe_subscription_cancel_failed`, `account.stripe_customer_delete_failed`, or `account.stripe_cleanup_retry_dispatch_failed`.

## What the system does
1. The API deletes the local user row after recording an `account.deleted` audit event.
2. Stripe cleanup runs synchronously.
3. If Stripe cleanup is partial, failed, or the Stripe client is unavailable, the API records `account.delete_partial_cleanup` and dispatches `maintenance.cleanup_stripe_orphan`.
4. The worker retries cleanup with exponential backoff: `30s, 60s, 120s, 240s, 480s`.

## Investigation steps
1. Look up the user deletion in `audit_events` using `event_type in ('account.deleted', 'account.delete_partial_cleanup', 'account.delete_failed')`.
2. Confirm whether the user row is already gone from the primary database.
3. Check worker logs for `maintenance.cleanup_stripe_orphan`.
4. Check whether Stripe cleanup retry dispatch succeeded or failed.

## Manual cleanup
1. Use the Stripe IDs from the audit metadata or logs.
2. Cancel any remaining active subscription in Stripe.
3. Delete or archive the Stripe customer if policy allows.
4. Record the manual cleanup action in the incident/support ticket with timestamp and operator name.

## Escalation
- Escalate to engineering if:
  - retry dispatch failed
  - cleanup retries exhausted
  - audit rows are missing or incomplete
  - a deleted account still appears billable after manual Stripe inspection
