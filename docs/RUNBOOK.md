# BacktestForecast Operational Runbook

## Health Checks

| Endpoint           | Purpose                     | Expected   |
|--------------------|-----------------------------|------------|
| `GET /health/live` | Process is alive            | `200 OK`   |
| `GET /health/ready`| DB + Redis reachable        | `200 OK`   |
| `GET /v1/meta`     | API version & environment   | `200 OK`   |

## Common Alerts

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
redis-cli KEYS "bff:rate-limit:*" | xargs redis-cli DEL
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

## Environment Variables

See `apps/api/.env.example` for the full list. Critical production variables:
- `DATABASE_URL` — PostgreSQL connection string
- `REDIS_URL` + `REDIS_PASSWORD` — Redis for Celery broker, rate limiting, SSE
- `CLERK_ISSUER`, `CLERK_AUDIENCE`, `CLERK_JWT_KEY` or `CLERK_JWKS_URL` — Auth
- `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET` — Billing
- `MASSIVE_API_KEY` — Market data
- `METRICS_TOKEN` — Prometheus endpoint protection
- `IP_HASH_SALT` — Must be non-default in production
