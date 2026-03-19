# BacktestForecast Operational Runbook

## Health Checks

| Endpoint           | Purpose                     | Expected   |
|--------------------|-----------------------------|------------|
| `GET /health/live` | Process is alive            | `200 OK`   |
| `GET /health/ready`| DB + Redis reachable        | `200 OK`   |
| `GET /v1/meta`     | API version & environment   | `200 OK`   |

## Account Deletion

### How it works

1. User calls `DELETE /v1/account/me` with header `X-Confirm-Delete: permanently-delete-my-account`
2. Rate limited to 1 per hour per user
3. In-flight Celery jobs are cancelled (DB status set to "cancelled" + Celery revoke)
4. Active Stripe subscription is cancelled via Stripe API
5. Stripe customer object is deleted via Stripe API
6. Audit event recorded with Stripe IDs for traceability
7. User row deleted — ON DELETE CASCADE removes all child records

### What to do if Stripe cleanup fails

If the Stripe cancellation or customer deletion fails (logged as `account.stripe_cleanup_failed`):
1. The user account is still deleted (Stripe failure does not block deletion)
2. Look up the orphan Stripe customer/subscription in the Stripe dashboard using the IDs from the audit event metadata
3. Cancel the subscription manually and delete or archive the customer

### How to find orphan Stripe customers

Query the `audit_events` table for `event_type = 'account.deleted'` and cross-reference the `metadata_json` Stripe IDs against active Stripe subscriptions.

## Common Alerts

### HealthReadyDegraded
**Cause:** The `/health/ready` endpoint is returning non-200 responses.

> **Note**: The `HealthReadyDegraded` alert requires a blackbox exporter.
> If using docker-compose.monitoring.yml, the blackbox-exporter service is
> included. Configure Prometheus to scrape it by adding a blackbox target
> in infra/prometheus/prometheus.yml.

1. Check `/health/ready` directly — it will report which dependency (DB or Redis) is unhealthy
2. Check PostgreSQL and Redis connectivity from the API pod
3. Review recent deployments or infrastructure changes

### StuckJobsHigh
**Cause:** Workers are down or Redis is unreachable.
1. Check worker pods/processes: `celery -A apps.worker.app.celery_app.celery_app inspect ping`
2. Check Redis connectivity
3. Check for queue backlog: `celery -A apps.worker.app.celery_app.celery_app inspect active`
4. Review worker logs for OOM or task timeouts

### HighTaskFailureRate
**Cause:** External API outage (Massive, Stripe), DB connection pool exhaustion, or code bug.
1. Check `celery_tasks_total{status="failed"}` to identify which task_name is failing
2. Check worker logs for the specific error
3. If Massive API is down, failures are expected; monitor for auto-recovery
4. If DB-related, check connection pool (`db_pool_size`, `db_pool_max_overflow`)

### HighHTTPErrorRate
**Cause:** Upstream dependency failure, deployment issue, or resource exhaustion.
1. Check `/health/ready` — if unhealthy, DB or Redis may be down
2. Review API logs filtered by `status >= 500`
3. Check recent deployments for regressions

### HighP95Latency
**Cause:** Slow queries, external API latency, or resource contention.
1. Check DB slow query log
2. Check Massive API response times
3. Review active connections: `SELECT count(*) FROM pg_stat_activity`

## Database Operations

### Run migrations
```bash
alembic upgrade head
```

### Check migration drift
```bash
python scripts/check_migration_drift.py
```

### Rollback last migration
```bash
alembic downgrade -1
```

## Worker Operations

### Restart workers
Workers are stateless — safe to restart at any time. In-flight tasks with
`task_acks_late=True` will be re-delivered.

### Purge a queue
```bash
celery -A apps.worker.app.celery_app.celery_app purge -Q <queue_name>
```

### Force-fail stuck jobs
The `maintenance.reap_stale_jobs` task runs every 10 minutes via Beat.
To trigger manually:
```bash
celery -A apps.worker.app.celery_app.celery_app call maintenance.reap_stale_jobs
```

## Reaper NameError Recovery

### Symptoms

- Celery task failures for `maintenance.reap_stale_jobs`
- Stuck jobs (running/queued) not being recovered
- ReaperTaskFailed alert firing

### Diagnosis

Check Celery worker logs for `NameError` referencing `stale_running_ids` (or similar variable name). This indicates a code bug where the reaper references an undefined variable.

### Fix

Deploy the latest code with the variable name fix.

### Recovery

Manually fail stuck jobs via SQL:

```sql
UPDATE backtest_runs SET status='failed' WHERE status='running' AND started_at < NOW() - INTERVAL '2 hours';
```

Adjust the table name and status column if your schema differs (e.g., `scanner_jobs`). Run for each affected job type.

## S3 Stream Leak Diagnosis

### Symptoms

- Slow export downloads
- boto3 connection pool warnings in logs
- S3ConnectionPoolExhausted alert firing

### Diagnosis

Check for `_stream_s3()` (or similar S3 streaming helpers) that do not call `body.close()` in a `finally` block. Unclosed response bodies hold connections in the pool until they are garbage-collected.

### Fix

Deploy the latest code with proper `finally: body.close()` (or context manager usage) around S3 stream reads.

### Recovery

Restart API workers to reset the connection pool. This releases leaked connections immediately.

## Redis Operations

### Check rate limiter health
```bash
redis-cli ping
```

### Clear rate limit counters (emergency)
```bash
redis-cli --scan --pattern "bff:rate-limit:*" | xargs redis-cli DEL
```

## Redis Failover Impact on Rate Limiting

### What happens

When Redis is unavailable, the rate limiter falls back to in-memory counters
(tracked by the `redis_rate_limit_fallback_total` metric). This means:

- Each API process maintains its own independent counters, so the effective
  rate limit is multiplied by the number of running processes.
- Counters are lost on process restart, allowing burst traffic immediately
  after a restart during an outage.
- The `bff_rate_limit_memory_counter_size` gauge tracks the number of entries
  in the in-memory fallback. If this grows unbounded, it can increase memory
  pressure on API pods.

### How to handle

1. **Monitor** `redis_rate_limit_fallback_total` — any non-zero rate means
   Redis is unreachable from at least one process.
2. **Check Redis** connectivity: `redis-cli -u $REDIS_URL ping`
3. **If Redis is permanently down**, consider scaling down API replicas to
   reduce the rate-limit multiplication effect until Redis is restored.
4. **After Redis recovery**, in-memory counters are automatically abandoned
   in favour of Redis on the next request. No manual action is needed.

## Database Pool Timeout Errors

### Symptoms

- `TimeoutError` or `QueuePool limit … reached` in API or worker logs during
  traffic spikes.
- Elevated `db_pool_checked_out` gauge approaching `db_pool_size`.

### Diagnosis

1. Check `pool_timeout` and `pool_pre_ping` in `src/backtestforecast/db/session.py`.
2. Review `db_pool_checked_out` and `db_pool_overflow` metrics.
3. Look for long-running transactions that hold connections open.

### Fix

- Increase `pool_timeout` (default 10 seconds) if spikes are transient.
- Increase `pool_size` or `max_overflow` for sustained load.
- Add more worker processes to spread connections across pools.
- Ensure all sessions are properly closed (context managers / `finally` blocks).

## Sweep job failures

### Symptoms
- Sweep jobs stuck in `queued` or `running` status
- Users report sweeps not completing

### Investigation
1. Check Celery worker logs for `sweeps.run` task errors
2. Query `sweep_jobs` table for jobs with status `failed` or `running` older than 1 hour
3. Check the DLQ for `sweeps.run` entries

### Resolution
- If a sweep is stuck in `running`, the reaper (`maintenance.reap_stale_jobs`) will mark it as `failed` after 30 minutes
- Failed sweeps can be retried by creating a new sweep with the same parameters
- Check worker memory and concurrency — sweeps are long-running and resource-intensive

## Environment Variables

See `apps/api/.env.example` for the full list. Critical production variables:
- `DATABASE_URL` — PostgreSQL connection string
- `REDIS_URL` + `REDIS_PASSWORD` — Redis for Celery broker, rate limiting, SSE
- `CLERK_ISSUER`, `CLERK_AUDIENCE`, `CLERK_JWT_KEY` or `CLERK_JWKS_URL` — Auth
- `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET` — Billing
- `MASSIVE_API_KEY` — Market data
- `METRICS_TOKEN` — Prometheus endpoint protection
- `IP_HASH_SALT` — Must be non-default in production

## Genetic Sweep Worker Starvation

**Symptom:** Worker queue depth grows, other tasks (backtests, scans, exports)
are delayed or timing out. `celery_tasks_total{task_name="sweeps.run"}` shows
high duration.

**Root Cause:** Genetic sweeps can run up to 3,600 seconds (1 hour). A burst
of genetic sweep requests can consume all worker slots.

**Diagnosis:**
1. Check active tasks: `celery -A apps.worker.app.celery_app inspect active`
2. Check queue depth: `redis-cli LLEN research`
3. Check sweep job durations in Grafana

**Resolution:**
1. If queue depth is critical, revoke long-running sweep tasks:
   `celery -A apps.worker.app.celery_app control revoke <task_id> --terminate`
2. Scale the worker pool temporarily: increase `--concurrency` or add workers
3. Consider lowering `SWEEP_GENETIC_TIMEOUT_SECONDS` or
   `max_generations` in the genetic config

**Prevention:**
- The `sweep_genetic_timeout_seconds` config controls the wall-clock limit
- Per-user rate limits (`sweep_create_rate_limit`) constrain burst requests
- Monitor `celery_tasks_total{task_name="sweeps.run",status="succeeded"}` duration

## Rate Limiter Bypass (Redis Down)

### What happens

When Redis is unavailable, the rate limiter **fails closed by default** (`RATE_LIMIT_FAIL_CLOSED=true`). Requests are rejected with `503 Service Unavailable` instead of falling back to in-memory counters. This ensures rate limits remain enforced even during Redis outages.

### What to do

1. **Monitor** `redis_rate_limit_fallback_total` — any non-zero rate means Redis is unreachable from at least one process.
2. **Check Redis** connectivity: `redis-cli -u $REDIS_URL ping`
3. **Restore Redis** — once Redis is healthy, the rate limiter automatically resumes using it. No manual action is needed.
4. **Emergency bypass** (use sparingly): Set `RATE_LIMIT_FAIL_CLOSED=false` to allow requests through with per-process in-memory fallback. This weakens rate limiting (effective limit is multiplied by the number of API processes). Only use if Redis will be down for an extended period and availability outweighs rate-limit enforcement.

## Daily Recommendations Cleanup

### Overview

The `maintenance.cleanup_daily_recommendations` task deletes old daily recommendations and their parent pipeline runs. It runs weekly via Celery Beat (schedule: `cleanup-daily-recommendations-weekly`).

### Verify the task is running

1. Check Celery Beat is running and the schedule includes `cleanup-daily-recommendations-weekly`.
2. Query the `daily_recommendations` table count: `SELECT count(*) FROM daily_recommendations`
3. Monitor `daily_recommendations_count` in Grafana — if it grows unbounded (> 50k), the cleanup may not be running.

### Manually trigger

```bash
celery -A apps.worker.app.celery_app.celery_app call maintenance.cleanup_daily_recommendations --kwargs='{"retention_days": 90}'
```

The task deletes recommendations older than `retention_days` (default 90) in batches.

## Billing Cancellation Failures

### Overview

When a subscription is revoked (e.g. via Stripe webhook), the billing service cancels all in-flight jobs (backtests, scans, exports, analyses, sweeps) for that user. Sweep jobs must be cancelled alongside other job types.

### Verify sweep jobs are cancelled

1. **Check webhook processing**: Ensure `customer.subscription.deleted` (or equivalent) is received and processed. Check API logs for `billing.in_flight_jobs_cancelled`.
2. **Query sweep_jobs**: `SELECT id, status, error_code, completed_at FROM sweep_jobs WHERE user_id = '<user_id>' AND status IN ('queued','running')` — should return no rows after cancellation.
3. **Check cancelled jobs**: `SELECT id, status, error_code FROM sweep_jobs WHERE user_id = '<user_id>' AND status = 'cancelled'` — verify `error_code = 'subscription_revoked'` and `completed_at` is set.
4. **Celery revoke**: The billing service revokes Celery tasks via `celery_app.control.revoke(..., terminate=True)`. If workers are unreachable, DB rows are still updated to `cancelled`; tasks may linger until worker restart.

### If sweeps are not cancelled

- Confirm the Stripe webhook is configured and reaching the API
- Check for `billing.celery_revoke_failed` or `billing.celery_import_unavailable` in logs
- Manually update stuck jobs: `UPDATE sweep_jobs SET status='cancelled', error_code='subscription_revoked', completed_at=NOW() WHERE user_id = '<user_id>' AND status IN ('queued','running')`

## Option Cache Staleness Incident

### Symptoms

- Users report stale or incorrect option data (e.g. strikes, expirations)
- Forecasts or backtests using outdated option chains
- `option_cache_entries` gauge high but data appears wrong

### Root Cause

The in-memory option gateway cache (`OptionDataRedisCache` or in-memory fallback) may serve stale data if:
- TTL (`option_cache_ttl_seconds`) is too long
- Redis cache was populated before a market data refresh
- Clock skew or cache invalidation logic bug

### Diagnosis

1. Check `option_cache_ttl_seconds` in config (default 7 days)
2. Check `option_cache_entries` — high count with user complaints suggests stale entries
3. Compare cached data to live Massive API response for a known symbol

### Fix

1. **Short-term**: Restart API pods to clear in-memory caches. Redis cache will repopulate from fresh fetches.
2. **Config**: Reduce `option_cache_ttl_seconds` if staleness is frequent (e.g. to 86400 for 24h).
3. **Code**: If invalidation is broken, deploy fix for cache invalidation on market data updates.
