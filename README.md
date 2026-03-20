# BacktestForecast.com Monorepo Scaffold

This repository contains the initial scaffold for:

- `apps/web`: Next.js 15 frontend
- `apps/api`: FastAPI API
- `apps/worker`: Celery worker and scheduler entrypoints
- `src/backtestforecast`: shared Python application package
- `alembic`: database migrations
- `packages/*`: reserved shared frontend packages

## What is intentionally not implemented yet

- Optimizer UX (parameter space sweeps beyond scanner)

## Implemented backend slices

The backend now supports:

- Clerk-authenticated user resolution
- `POST /v1/backtests` for an async Celery-backed manual backtest run (returns 202 with queued status)
- `GET /v1/backtests` for history (includes queued/running/succeeded/failed runs)
- `GET /v1/backtests/{run_id}` for full detail (supports polling for in-progress runs)
- `POST /v1/scans` to create a queued scanner job
- `GET /v1/scans`, `GET /v1/scans/{job_id}`, and `GET /v1/scans/{job_id}/recommendations`
- `GET /v1/forecasts/{ticker}` for a bounded historical-analog range
- Long Call, Long Put, Covered Call, Cash-Secured Put, vertical spreads, Iron Condor, Long Straddle, Long Strangle, Calendar Spread, Butterfly, Wheel, Poor Man's Covered Call, Ratio Call/Put Backspread, Collar, Diagonal Spread, Double Diagonal, Short Straddle, Short Strangle, Covered Strangle, Synthetic Put, Reverse Conversion, Jade Lizard, Iron Butterfly, and Custom 2/3/4/5/6/8-leg strategies
- 33 total strategies across 9 categories: single-leg, income, vertical spreads, multi-leg, short volatility, diagonal, ratio, synthetic/exotic, custom
- Custom N-leg strategies accept user-defined leg definitions with strike offsets (relative to ATM), expiration offsets, quantity ratios, and mixed option/stock legs
- RSI, SMA/EMA crossover, MACD, Bollinger Bands, IV Rank / IV Percentile, volume spike, support/resistance, and avoid earnings date entry rules
- Persistent run, trade, equity curve, scanner job, and ranked recommendation storage
- Daily-bar underlying evaluation with option mid-price fills
- Celery-backed backtest execution with queued/running/succeeded/failed lifecycle
- Celery-backed scanner execution and daily refresh scheduling
- Idempotent backtest submission via optional `idempotency_key`
- Frontend polling for in-progress backtest runs
- `POST /v1/templates`, `GET /v1/templates`, `GET /v1/templates/{id}`, `PATCH /v1/templates/{id}`, `DELETE /v1/templates/{id}` for saved backtest templates
- Per-tier template limits (Free: 3, Pro: 25, Premium: 100)
- Frontend template picker on the new-backtest form and "Save as template" action
- Templates page at `/app/templates` with list, apply, and delete
- Stripe checkout sessions, billing portal, and webhook handler for subscription lifecycle
- Backend-authoritative plan enforcement: `quota_exceeded` for usage limits, `feature_locked` for tier-gated features
- Webhook middleware bypass for Stripe signature verification
- Frontend upgrade prompts when plan limits are hit (replaces generic error messages)
- Post-checkout success banner on the billing settings page
- Dashboard quota-approaching warnings for free-tier users
- `POST /v1/backtests/compare` for side-by-side comparison of 2–10 runs, enforced by `side_by_side_comparison_limit`
- Frontend compare page at `/app/backtests/compare?ids=a,b` with metrics table, overlaid equity curves, and trade counts
- Selectable checkboxes on the history page for selecting runs to compare
- Scanner frontend: dashboard with job history, new-scan form, job detail with polling, ranked recommendation display
- Scanner form supports symbol input, strategy type checkboxes, rule set configuration, and all backtest parameters
- Celery-backed export generation (`exports.generate` task on `exports` queue) for CSV and PDF
- `POST /v1/exports` returns 202 with queued status; `GET /v1/exports/{id}/status` for polling; `GET /v1/exports/{id}` for download
- Frontend export buttons poll for completion then auto-download the generated file
- `GET /v1/strategy-catalog` returns grouped metadata for all 33 strategies with labels, descriptions, categories, bias, leg count, tier requirements, and max-loss descriptions
- Backtest form strategy selector shows all 33 strategies in grouped optgroups with strategy detail card (bias, leg count, tier badge, description, max loss)
- Forecast page at `/app/forecasts` with ticker lookup, strategy context, horizon selector, and visual range display (Pro+ gated)
- Nightly scan pipeline: 5-stage funnel (universe screen → regime-strategy matching → quick backtest → full backtest → forecast+rank) producing top daily trade recommendations
- `GET /v1/daily-picks` and `GET /v1/daily-picks/history` for pipeline results (Pro+ gated)
- Daily Picks page at `/app/daily-picks` with ranked recommendation cards, pipeline stats, regime badges, and forecast overlays
- Pipeline runs nightly at 6:00 AM UTC via Celery beat on the `pipeline` queue
- Default universe: 100 optionable symbols across tech, finance, healthcare, energy, industrials, consumer, media, and high-volatility names
- Single-symbol deep analysis: exhaustive testing of 27 strategies × 36 parameter configs (972 quick backtests), regime classification, top-10 full backtests with forecast overlay
- `POST /v1/analysis`, `GET /v1/analysis/{id}`, `GET /v1/analysis/{id}/status`, `GET /v1/analysis` for deep analysis CRUD + polling
- Deep Analysis page at `/app/analysis` with form, progress polling (shows current stage), regime display, strategy landscape, and ranked top results

## Vertical-slice modeling notes

These are implementation assumptions for the current slices:

- Entries are evaluated on daily closes.
- The option price for entry/mark/exit uses the latest same-day quote mid.
- Strike selection is nearest-to-spot on entry with a small tie bias toward near-ATM/OTM.
- Exits happen on the earliest of:
  - expiration
  - `max_holding_days`
  - the final available bar after the requested backtest end
- A single open position is allowed at a time in a manual backtest.
- Scanner ranking is deterministic and historical-performance weighted.
- Forecast ranges are probabilistic historical analogs, not predictions or advice.

## Local setup

1. Copy env files:
   - `cp apps/api/.env.example apps/api/.env`
   - `cp apps/web/.env.example apps/web/.env.local`

2. Start local infrastructure:
   - `docker compose up -d postgres redis`

3. Create Python virtualenv and install backend deps:
   - `python3.12 -m venv .venv`
   - `source .venv/bin/activate`
   - `pip install --upgrade pip`
   - `pip install -e .[dev]`

4. Run migrations:
   - `alembic upgrade head`

5. Install frontend deps:
   - `pnpm install`

6. Start services in separate terminals:
   - `./scripts/dev-api.sh`
   - `./scripts/dev-worker.sh`
   - `./scripts/dev-beat.sh`
   - `./scripts/dev-web.sh`

## Health checks

- API live: `http://localhost:8000/health/live`
- API ready: `http://localhost:8000/health/ready`
- Web health: `http://localhost:3000/api/health`

## Notes

- API and worker import shared code from `src/backtestforecast`.
- Manual backtest runs are now dispatched to Celery via the `backtests.run` task on the `research` queue.
- `POST /v1/backtests` returns HTTP 202 with the run in `queued` status; the frontend polls `GET /v1/backtests/{run_id}` until the status becomes `succeeded` or `failed`.
- Scanner jobs execute asynchronously through Celery.
- Daily scan refresh creates a new child job instead of mutating the original snapshot.
- Idempotent submission is supported via an optional `idempotency_key` field on the create request.
- Stripe webhooks are received at `POST /v1/billing/webhook` — this path bypasses body-limit middleware and Clerk auth.
- Plan tier is synced from Stripe subscription events. The webhook handler is idempotent (duplicate events are detected via audit log).
- Entitlements are backend-authoritative: the API raises `quota_exceeded` (403) for usage limits and `feature_locked` (403) for tier-gated features.
- To configure Stripe locally, set `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, and the four price ID env vars. Without these, billing endpoints return `ConfigurationError`.
- Side-by-side comparison limit is per-tier: Free = 1 (effectively disabled), Pro = 3, Premium = 8.
- Export jobs are dispatched to the `exports` Celery queue. CSV and PDF generation runs in the worker, not the API process.
- Health endpoints include `version` and `environment` fields for debugging in deployed environments.
- Beat should use the same worker Docker image with a CMD override — must be a singleton.

## Documentation

- `docs/launch-readiness-checklist.md` — comprehensive pre-launch checklist
- `docs/deployment-production.md` — topology, env vars, deploy sequence, rollback
- `docs/security-review-checklist.md` — application and infrastructure security items
- `docs/test-plan.md` — 55-test automated suite + manual pre-launch checks
- `docs/monitoring-alerting.md` — metrics, alerts, and dashboards
- `docs/failure-mode-review.md` — failure matrix with operator actions
- `docs/data-provider-outage-strategy.md` — provider degradation playbook
- `docs/backtest-strategy-assumptions.md` — modeling assumptions for all 14 strategies
- `docs/scanner-assumptions.md` — scanner ranking and forecast methodology
- `docs/conventions.md` — architecture, product invariants, and error code conventions
- `docs/known-limitations.md` — current known gaps
- `docs/recommended-next-10-product-improvements.md` — backlog priorities
