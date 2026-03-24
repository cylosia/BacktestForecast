# Monitoring and alerting recommendations

## Minimum telemetry
- Structured request logs with `request_id`, route, method, status code, duration.
- Worker task logs with job id, user id, candidate count, and recommendation count.
- Audit logs for billing changes, account deletions, and export downloads.

## Metrics to emit
- API request count / error rate / p95 latency by route.
- Backtest creation success vs failure rate.
- Scanner queue depth, scanner job age, scan completion latency.
- Stripe webhook success vs duplicate vs failure counts.
- Export generation success/failure counts.
- Billing audit write failures and replay throughput (`billing_audit_write_failures_total`, `billing_audit_replayed_total`).
- Idempotent duplicate returns by job status (`idempotent_duplicate_returns_total`).
- Stale queued duplicate repairs (`stale_queued_duplicate_returns_total`).
- Provider request latency, retry count, and provider error rate.
- Redis availability and fallback-to-memory rate limiting count.
- External cleanup failures (`external_cleanup_failures_total`) for post-delete Stripe cleanup and retry-dispatch failures.

## Recommended alerts
- API 5xx rate > 2% for 10 minutes.
- `/health/ready` degraded for 5+ minutes.
- Scanner queue oldest job > 10 minutes.
- Stripe webhook failures > 3 in 15 minutes.
- `billing_audit_write_failures_total` increasing over 10 minutes, indicating billing state changes are being deferred to fallback storage.
- `stale_queued_duplicate_returns_total` repeatedly increasing for the same job type over 15 minutes.
- `idempotent_duplicate_returns_total{status="queued"}` above baseline, indicating users are repeatedly hitting queued duplicates.
- Massive provider 429/5xx spike above baseline.
- Export failure rate > 5% in 30 minutes.
- `external_cleanup_failures_total{resource=~"stripe_subscription|stripe_customer|stripe_cleanup_retry"}` increasing, indicating account deletions are leaving orphaned Stripe cleanup work behind.
- Repeated stuck-job alerts (`StuckJobsHigh`, `ScannerJobStuckRunning`, `SweepJobStuckRunning`) should route to the stuck-jobs support runbook first, then to worker/on-call escalation if manual cancellation or reaper recovery fails.

## Dashboards
- **Exec summary**: signups, paid users, backtests/day, scans/day, exports/day.
- **API health**: request volume, latency, 4xx/5xx, ready-state.
- **Worker health**: queue depth, retries, job durations.
- **Provider health**: success rate, retry volume, outage periods.
- Queue diagnostics:
  - `/health/ready` and `/admin/dlq` expose `queue_diagnostics` with `stale_queued_total` and `stale_without_outbox_total`.
  - Prometheus emits `queued_jobs_past_dispatch_sla{model=...}` and `queued_jobs_without_outbox{model=...}` from the metrics scrape path.
  - `job_create_to_running_latency_seconds{model=...}` tracks create-to-running latency for synthetic and alerting thresholds.
- Billing cleanup diagnostics:
  - `external_cleanup_failures_total` captures synchronous Stripe cleanup failures and retry-dispatch failures after account deletion.
  - `maintenance.cleanup_stripe_orphan` retries with exponential backoff (30s, 60s, 120s, 240s, 480s), soft time limit 60s, hard time limit 90s.
