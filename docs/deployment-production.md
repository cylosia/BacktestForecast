# Production deployment guide

## Recommended topology
- **Web**: Next.js standalone build on a Node container or edge runtime.
- **API**: FastAPI container (`apps/api/Dockerfile`).
- **Worker**: Celery worker container (`apps/worker/Dockerfile`) — processes `research,exports,maintenance,pipeline` queues.
- **Beat**: Same worker image with CMD override: `celery -A apps.worker.app.celery_app.celery_app beat --loglevel=INFO`. Must be a singleton.
- **Stateful services**: PostgreSQL + Redis.

## Required environment groups
### API / worker / beat
- `DATABASE_URL`
- `REDIS_URL`
- `APP_ENV=production`
- `APP_PUBLIC_URL`
- `API_PUBLIC_URL`
- `WEB_CORS_ORIGINS`
- `API_ALLOWED_HOSTS`
- `REQUEST_MAX_BODY_BYTES`
- Clerk verification env vars
- Stripe secret + webhook secret + price ids
- `MASSIVE_API_KEY`
- `LOG_JSON=true`

### Web
- `NEXT_PUBLIC_APP_URL`
- `NEXT_PUBLIC_API_BASE_URL`
- Clerk publishable + secret keys
- Clerk sign-in redirect settings

## Deployment sequence
1. Provision PostgreSQL and Redis.
2. Apply env vars to API, worker, beat, and web.
3. Run Alembic migrations before shifting traffic.
4. Deploy API.
5. Deploy worker and beat.
6. Deploy web.
7. Run a smoke test:
   - `/health/live`
   - `/health/ready`
   - authenticated `/v1/me`
   - create one backtest
   - create one scanner job
   - create one CSV export

## Operational notes
- Scale **API** on request rate and p95 latency.
- Scale **worker** on queued scan depth and job age.
- Keep **beat** singleton to avoid duplicate scheduled refresh launches.
- Use blue/green or rolling deployment with migration-first sequencing.

## Connection pool sizing

The API and worker each maintain a SQLAlchemy connection pool to PostgreSQL. Two engines exist: a default engine (30 s statement timeout) for API requests, and a worker engine (300 s) for long-running tasks. Both engines share the same pool settings.

| Env var | Default | Description |
|---------|---------|-------------|
| `DB_POOL_SIZE` | `5` | Base number of persistent connections per engine |
| `DB_POOL_MAX_OVERFLOW` | `10` | Extra connections allowed above `pool_size` under load |
| `DB_POOL_RECYCLE` | `1800` | Seconds before a connection is recycled (prevents stale connections after PG restarts) |
| `DB_POOL_TIMEOUT` | `10` | Seconds to wait for a connection from the pool before raising |

### Capacity math

Each OS process creates its own engine, so the total connection count depends on the number of processes:

- **API**: `uvicorn --workers N` × `pool_size` = base connections. With default `--workers 4` and `pool_size=5`: 20 base, up to 60 with overflow.
- **Worker**: `WORKER_REPLICAS` × `pool_size` = base connections. With default 2 replicas and `pool_size=5`: 10 base, up to 30 with overflow.
- **Beat**: 1 process, rarely opens DB connections.

**Total worst-case**: ~90 connections against PostgreSQL's default `max_connections=100`. If you increase uvicorn workers or worker replicas, increase `max_connections` on PostgreSQL accordingly, or reduce `DB_POOL_SIZE`/`DB_POOL_MAX_OVERFLOW`.

### Recommended production values

For a moderate-load deployment (4 API workers, 2 worker replicas):

```
DB_POOL_SIZE=5
DB_POOL_MAX_OVERFLOW=10
DB_POOL_RECYCLE=1800
DB_POOL_TIMEOUT=10
```

For higher load (8 API workers, 4 worker replicas), either increase PostgreSQL `max_connections` to 200+ or reduce per-process pool size:

```
DB_POOL_SIZE=3
DB_POOL_MAX_OVERFLOW=7
DB_POOL_RECYCLE=1800
DB_POOL_TIMEOUT=10
```

`pool_pre_ping=True` is always enabled to detect stale connections after DB restarts.

## Enabling E2E tests in CI

The `e2e-tests` job in `.github/workflows/ci.yml` is opt-in. To enable it, configure the following in your GitHub repository settings:

### Repository variables (Settings > Variables > Actions)

| Variable | Value |
|----------|-------|
| `ENABLE_E2E_TESTS` | `true` |

### Repository secrets (Settings > Secrets > Actions)

| Secret | Source |
|--------|--------|
| `CLERK_TESTING_TOKEN` | Clerk dashboard > Testing tokens |
| `CLERK_TEST_EMAIL` | Email of a dedicated test account |
| `CLERK_TEST_PASSWORD` | Password for the test account |
| `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY` | Clerk dashboard > API keys |
| `CLERK_SECRET_KEY` | Clerk dashboard > API keys |

The `ci.yml` E2E job starts only the Next.js frontend. The separate `playwright.yml` workflow (triggered on PRs touching `apps/web/**`) spins up the full stack (Postgres, Redis, API, web) for deeper coverage.

## Monitoring stack

A monitoring compose file is provided at `docker-compose.monitoring.yml` with Prometheus, Alertmanager, and Grafana:

```bash
docker compose -f docker-compose.prod.yml -f docker-compose.monitoring.yml up -d
```

Before deploying:
1. Create `infra/prometheus/secrets/metrics_token` containing the `METRICS_TOKEN` value.
2. Customize `infra/alertmanager/alertmanager.yml` with your receiver config (Slack, PagerDuty, email, etc.).
3. Alert rules are at `infra/grafana/alerts/rules.yml` and are auto-loaded by Prometheus.

## Rollback
- Roll back web and API images independently.
- If a migration is non-breaking, keep schema and roll back code first.
- If the provider layer is failing, disable scanner launch from the UI and keep history/read routes available.
