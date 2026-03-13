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

## Redis Operations

### Check rate limiter health
```bash
redis-cli ping
```

### Clear rate limit counters (emergency)
```bash
redis-cli KEYS "bff:rate-limit:*" | xargs redis-cli DEL
```

## Environment Variables

See `apps/api/.env.example` for the full list. Critical production variables:
- `DATABASE_URL` — PostgreSQL connection string
- `REDIS_URL` + `REDIS_PASSWORD` — Redis for Celery broker, rate limiting, SSE
- `CLERK_ISSUER`, `CLERK_AUDIENCE`, `CLERK_JWT_KEY` or `CLERK_JWKS_URL` — Auth
- `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET` — Billing
- `MASSIVE_API_KEY` — Market data
- `METRICS_TOKEN` — Prometheus endpoint protection
- `IP_HASH_SALT` — Must be non-default in production
