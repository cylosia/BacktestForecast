# Launch readiness checklist

## Database and migrations
- [ ] Alembic migrations applied in staging: `alembic upgrade head`
- [ ] Alembic migrations applied in production: `alembic upgrade head`
- [ ] PostgreSQL backups configured and restore tested

## Authentication
- [ ] Clerk env vars set: `CLERK_JWT_KEY` or `CLERK_JWKS_URL`, `CLERK_ISSUER`, `CLERK_AUTHORIZED_PARTIES`
- [ ] Clerk auth validated end-to-end: sign-in → `/app/dashboard` → `/v1/me` returns user
- [ ] Clerk route protection works: unauthenticated `/app/*` redirects to sign-in

## Billing
- [ ] Stripe env vars set: `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, all 4 price IDs
- [ ] `GET /v1/meta` shows `billing_enabled: true`
- [ ] Stripe checkout flow validated in test mode: Free → Pro upgrade completes
- [ ] Stripe webhook delivery confirmed: `POST /v1/billing/webhook` returns `{"status": "ok"}`
- [ ] Webhook duplicate detection works: second delivery returns `{"status": "duplicate"}`
- [ ] Portal session opens the Stripe customer portal
- [ ] Plan tier syncs correctly: `user.plan_tier` updates after webhook

## Core features — smoke tests
- [ ] One manual backtest succeeds end-to-end in staging (POST → queued → running → succeeded)
- [ ] Backtest detail page renders with trades, equity curve, and summary metrics
- [ ] One scanner job succeeds end-to-end in staging (POST → Celery → recommendations)
- [ ] Scanner recommendations page renders with scores and forecasts
- [ ] One CSV export downloads successfully
- [ ] One PDF export downloads successfully
- [ ] Side-by-side comparison renders for 2 runs
- [ ] Template CRUD works: create → list → apply → delete
- [ ] Forecast lookup returns a range for a supported ticker
- [ ] Strategy catalog returns all 33 strategies

## Quota and entitlement enforcement
- [ ] Free tier: 6th backtest in a month returns `quota_exceeded` (403)
- [ ] Free tier: scanner returns `feature_locked` (403)
- [ ] Free tier: export returns `feature_locked` (403)
- [ ] Free tier: forecast returns `feature_locked` (403)
- [ ] Free tier: comparing 2 runs returns `feature_locked` (403)
- [ ] Free tier: 4th template returns `quota_exceeded` (403)
- [ ] Pro tier: backtests are unlimited, scanner basic works, CSV export works
- [ ] Premium tier: all features unlocked

## Health and observability
- [ ] `/health/live` returns `{"status": "ok"}` — connected to monitoring/uptime checker
- [ ] `/health/ready` returns database and redis status — connected to load balancer health check
- [ ] `/health/ready` includes `version` and `environment` fields
- [ ] API request logs include `request_id`, path, method, status, and duration (structured JSON in production)
- [ ] Worker task logs include job ID and outcome
- [ ] Audit events are being written for billing changes and export downloads
- [ ] `LOG_JSON=true` is set in production/staging

## Security
- [ ] `APP_ENV=production` in production
- [ ] `API_ALLOWED_HOSTS` does not include `*`
- [ ] `WEB_CORS_ORIGINS` is restricted to the web app origin
- [ ] `REQUEST_MAX_BODY_BYTES` is set (default 1MB)
- [ ] Webhook path (`/v1/billing/webhook`) bypasses body-limit middleware (already implemented)
- [ ] CSV exports are formula-safe (cells starting with `=`, `+`, `-`, `@` are prefixed)
- [ ] Secrets are in a secret manager, not committed to source

## Docker and deployment
- [ ] API Dockerfile builds and runs: `docker build -f apps/api/Dockerfile .`
- [ ] Web Dockerfile builds with `output: "standalone"`: `docker build -f apps/web/Dockerfile .`
- [ ] Worker Dockerfile processes all queues: `research,market_data,exports,maintenance`
- [ ] Beat uses same worker image with CMD override: `celery ... beat --loglevel=INFO`
- [ ] Beat is a singleton (one instance only)
- [ ] Blue/green or rolling deploy with migration-first sequencing

## CI
- [ ] `pytest -q` passes
- [ ] Python compile smoke check passes
- [ ] Web lint + typecheck pass
- [ ] API app import smoke check passes

## Operational readiness
- [ ] On-call owner identified
- [ ] Rollback procedure documented (see `docs/deployment-production.md`)
- [ ] Provider outage playbook reviewed (see `docs/data-provider-outage-strategy.md`)
- [ ] Failure mode matrix reviewed (see `docs/failure-mode-review.md`)
