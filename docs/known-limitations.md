# Known limitations

- Market-data failover is still single-provider in practice; retries reduce pain but do not provide true redundancy.
- Webhook dedupe uses the `stripe_events` table with a `uq_stripe_events_event_id` unique constraint for reliable event deduplication.
- Integration tests require PostgreSQL (via Docker or a local instance); there is no SQLite fallback.
- The scanner still relies on deterministic ranking heuristics rather than a learned ranking model.
- The strategy catalog is served from an in-process Python module; a more dynamic catalog with user-configurable parameters may follow.
- `slippage_pct` was previously not wired to the API and defaulted to 0%. It is now configurable via the backtest request payload. Backtests run before this change used no slippage adjustment; historical results are not retroactively corrected.
- Naked option positions (naked calls/puts) are sized by margin requirement only. This understates the theoretical risk, which is unlimited for naked calls and strike-minus-zero for naked puts. Users should interpret naked-option backtest results with caution and apply their own risk limits.
- `RISK_FREE_RATE` is a static environment variable (default 0.045). Sharpe and Sortino ratio calculations use this value without date sensitivity. When the prevailing risk-free rate changes, the env var must be updated manually and backtests re-run to reflect current conditions.
- The frontend enforces `target_dte >= 7` while the backend schema allows `target_dte >= 1`. This is intentional — sub-weekly DTE options typically have insufficient liquidity for meaningful backtesting. The API will accept DTE 1-6 from programmatic clients, but the web UI prevents it. See `apps/web/lib/backtests/validation.ts` line 116.
- `market_date_today()` uses a hybrid holiday set: a static fallback covering 2025-2027 plus dynamic holidays fetched weekly from the Massive `/v1/marketstatus/upcoming` endpoint and cached in Redis. If the Massive API or Redis is unavailable, the static set is used alone. The static set should still be extended periodically as a safety net.
- Sortino ratio uses a sample-corrected denominator (N-1) for internal consistency with the Sharpe ratio calculation. Some academic references use a population denominator (N); results may differ slightly from external tools that use the population formula.

## SSE Infrastructure

The backend implements a full Server-Sent Events (SSE) infrastructure (`apps/api/app/routers/events.py`) with Redis Pub/Sub, connection slot management via Lua scripts, and an SSE proxy route in the Next.js frontend (`apps/web/app/api/events/[...path]/route.ts`).

The frontend has active SSE consumers through `useSSE` in the backtest, scanner, and sweep job pollers, with polling fallback when the stream fails. This means SSE is part of the live real-time path and should be tested/operated accordingly rather than treated as dormant infrastructure.

This infrastructure consumes Redis connection pool resources, so capacity and proxy buffering settings still need to be monitored in production.

## OutboxMessage Scaffolding

The `OutboxMessage` model and its associated table exist as infrastructure scaffolding for a transactional outbox pattern. The `dispatch.py` module handles task dispatch with inline retries but does **not** create `OutboxMessage` rows. The `poll_outbox` beat task has been disabled since the table is always empty. The `cleanup_outbox` task still runs daily to clean up any manually inserted rows. Remove this infrastructure if not implemented by 2026-06-01.

## Export Storage

Exports are stored using `DatabaseStorage` (PostgreSQL `LargeBinary` column) by default. For production deployments with significant export volume, configure S3 storage:

```env
S3_BUCKET=your-bucket-name
S3_REGION=us-east-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

When S3 is configured, new exports are written to S3 and existing DB-stored exports continue to work. A periodic `reconcile_s3_orphans` task cleans up S3 objects that no longer have a corresponding database record.

**Migration note:** There is no automatic migration of existing DB-stored exports to S3. Exports will continue to be served from the database until they expire (30 days).

## Scan Timeout Interactions

The scanner has multiple timeout layers:

1. **`scan_timeout_seconds`** (default 540s / 9 minutes): The overall scan execution budget. When approaching this limit, remaining candidates are skipped.
2. **`_CANDIDATE_TIMEOUT_SECONDS`** (120s): Reserved buffer subtracted from the scan timeout. The scan stops accepting new candidates when `elapsed > scan_timeout - candidate_timeout`.
3. **`soft_time_limit`** (600s / 10 minutes): Celery soft timeout that sends `SoftTimeLimitExceeded`. The scan should complete before this fires.
4. **`time_limit`** (660s / 11 minutes): Celery hard kill. The worker process is terminated.
5. **`statement_timeout`** (300s / 5 minutes): PostgreSQL query timeout for worker sessions. Individual queries exceeding this are cancelled.

The intended hierarchy is: `scan_timeout < soft_time_limit < time_limit`, and all individual queries must complete within `statement_timeout`.

## Commit-First Dispatch Gap

The job creation flow commits the DB record (with `celery_task_id`) before
sending the Celery task to the broker. If the API process crashes between
the commit and the `send_task` call, the job stays in "queued" status with
a `celery_task_id` that was never dispatched. The reaper task
(`maintenance.reap_stale_jobs`, runs every 10 minutes) detects these stuck
jobs and either re-dispatches them or marks them as failed after 30 minutes.

This means there is a worst-case 30-minute window where a user sees a
"queued" job that is not progressing. The `OutboxMessage` table exists as
infrastructure for a future transactional outbox pattern that would
eliminate this gap entirely.

## Option Data Cache Staleness

The option data cache uses a 7-day TTL (`option_cache_ttl_seconds`). During this
window, cached option chain data is served without checking whether the upstream
provider has fresher data. This means:

- Recently listed or delisted options may not appear/disappear for up to 7 days
- Corporate actions (splits, dividends) affecting option chains may use stale data
- Implied volatility and greeks are snapshot values, not live

For backtesting historical scenarios this is acceptable. For near-real-time
analysis (daily picks, forecasts), consider reducing the TTL or adding a
cache-bust mechanism.

## Migration Downgrades

Some migrations include destructive downgrade paths:
- `0017` drops `plan_tier_snapshot` column on downgrade
- `0008` changes `idempotency_status` from `processing` to `error` on downgrade
- `0022` drops the `mode` column from `sweep_jobs` on downgrade

Always test downgrade paths in staging before applying in production.
Prefer forward-fixing (new migration) over downgrading in production.

## Genetic Sweep Convergence

Genetic sweeps with degenerate parameter spaces (e.g., very few valid
combinations) may not converge within the configured max_generations.
The sweep will still complete with the best results found, but the
`generations_run` field in the result may equal `max_generations`,
indicating premature termination.

Long-running genetic sweeps can also starve other job types. Consider
configuring a dedicated Celery queue for sweep tasks if this becomes
an issue.
