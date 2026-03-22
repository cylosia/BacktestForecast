# Monitoring and alerting recommendations

## Minimum telemetry
- Structured request logs with `request_id`, route, method, status code, duration.
- Worker task logs with job id, user id, candidate count, and recommendation count.
- Audit logs for billing changes and export downloads.

## Metrics to emit
- API request count / error rate / p95 latency by route.
- Backtest creation success vs failure rate.
- Scanner queue depth, scanner job age, scan completion latency.
- Stripe webhook success vs duplicate vs failure counts.
- Export generation success/failure counts.
- Idempotent duplicate returns by job status (`idempotent_duplicate_returns_total`).
- Stale queued duplicate repairs (`stale_queued_duplicate_returns_total`).
- Provider request latency, retry count, and provider error rate.
- Redis availability and fallback-to-memory rate limiting count.

## Recommended alerts
- API 5xx rate > 2% for 10 minutes.
- `/health/ready` degraded for 5+ minutes.
- Scanner queue oldest job > 10 minutes.
- Stripe webhook failures > 3 in 15 minutes.
- `stale_queued_duplicate_returns_total` repeatedly increasing for the same job type over 15 minutes.
- `idempotent_duplicate_returns_total{status="queued"}` above baseline, indicating users are repeatedly hitting queued duplicates.
- Massive provider 429/5xx spike above baseline.
- Export failure rate > 5% in 30 minutes.

## Dashboards
- **Exec summary**: signups, paid users, backtests/day, scans/day, exports/day.
- **API health**: request volume, latency, 4xx/5xx, ready-state.
- **Worker health**: queue depth, retries, job durations.
- **Provider health**: success rate, retry volume, outage periods.
- Queue diagnostics:
  - `/health/ready` and `/admin/dlq` now expose `queue_diagnostics` with `stale_queued_total` and `stale_without_outbox_total`.
  - Prometheus emits `queued_jobs_past_dispatch_sla{model=...}` and `queued_jobs_without_outbox{model=...}` from the metrics scrape path.
  - `job_create_to_running_latency_seconds{model=...}` tracks create→running latency for synthetic/alerting thresholds.
