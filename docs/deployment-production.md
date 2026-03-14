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

## Rollback
- Roll back web and API images independently.
- If a migration is non-breaking, keep schema and roll back code first.
- If the provider layer is failing, disable scanner launch from the UI and keep history/read routes available.
