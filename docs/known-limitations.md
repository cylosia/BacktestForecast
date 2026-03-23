# Known limitations

- Market-data failover is still single-provider in practice; retries reduce pain but do not provide true redundancy.
- Webhook dedupe uses the `stripe_events` table with a `uq_stripe_events_event_id` unique constraint for reliable event deduplication.
- Integration tests require PostgreSQL (via Docker or a local instance); there is no SQLite fallback.
- The scanner still relies on deterministic ranking heuristics rather than a learned ranking model.
- The strategy catalog is served from an in-process Python module; a more dynamic catalog with user-configurable parameters may follow.
- `slippage_pct` is now configurable via the backtest request payload; historical runs created before that rollout still reflect zero slippage.
- Naked option positions (naked calls/puts) are still collateral-sized by margin requirement, not by worst-case stress loss. API payloads now emit explicit `naked_option_margin_only` warnings, but users should still treat those runs as economically aggressive and apply separate stress-loss sizing limits outside the current model.
- Sharpe/Sortino payloads now surface an explicit `configured_static_risk_free_rate` warning whenever the request relies on the server-configured risk-free rate. The configured rate is mechanically consistent inside a run, but it is still not a Treasury series dynamically matched to the historical window.
- `market_date_today()` still relies on a hybrid holiday set: static fallback dates plus dynamically refreshed upstream holidays.
- Sortino ratio uses a sample-corrected denominator (N-1), so some external tools may differ slightly.

## Current operational assumptions

`docs/current-operational-assumptions.md` is the authoritative current-state entry point for runtime behavior and operator assumptions. Use that page plus `docs/workflow-trace.md` for dispatch, auth, queue, and recovery semantics; keep this document focused on still-open limitations rather than historical architecture drift.

See `docs/README.md` for the split between current operational docs and historical audit archives.

## SSE Infrastructure

The SSE stack is live infrastructure: the FastAPI events router, Redis Pub/Sub fanout, and the Next.js proxy route are all active and backtest/scan/sweep pollers use SSE with polling fallback. Operators should treat SSE capacity, buffering, and Redis connection limits as production concerns.

## Dispatch / Outbox Behavior

The asynchronous create flows now persist queued job state together with outbox metadata and then attempt inline broker delivery. If inline delivery fails, the job remains queued and the pending outbox row is available for recovery. Beat-driven outbox and stale-job repair paths are part of the live runtime, so delayed jobs should be investigated through current dispatch metrics/runbooks rather than older "commit-first gap" assumptions.

## Export Storage

Exports can still be stored in PostgreSQL `content_bytes` when S3 is not configured. That fallback remains operationally useful, but it is not ideal for large-download workloads because database-backed files are still read into Python memory before the response is streamed. S3-backed exports remain the preferred production path for larger files or higher concurrency.

## Scan Timeout Interactions

The scanner still has multiple timeout layers (`scan_timeout_seconds`, candidate buffer, Celery soft/hard limits, and PostgreSQL statement timeout). Those must remain ordered correctly to avoid partial-result or hard-kill behavior.

## Option Data Cache Staleness

The option data cache still uses a 7-day TTL (`option_cache_ttl_seconds`). For near-real-time analysis, that can leave newly listed/delisted contracts or fresher greeks implied-volatility snapshots stale for days.

## Migration Downgrades

Some migrations still include destructive downgrade paths. Prefer forward-fixing with new migrations over downgrading in production, and test downgrades in staging before relying on them.

## Genetic Sweep Convergence

Genetic sweeps with degenerate parameter spaces can still terminate at `max_generations` without strong convergence, and long-running sweep workloads can still starve other queues without dedicated worker isolation.

## Deployment Validation and Redis Topology

CI now runs lightweight load-test contract checks, and CD can run staged authenticated smoke workflows plus a short Locust run when staging tokens are configured. That still depends on operators wiring the staging secrets/variables; health-only deploy verification should no longer be treated as sufficient evidence of workflow safety.

`docker-compose.prod.yml` remains safe only for single-host/private-network Redis traffic. If Redis leaves that boundary, switch to the TLS-ready `docker-compose.prod.tls.yml` override plus `infra/redis/redis.tls.conf`, and treat `rediss://` URLs and certificate mounts as mandatory rather than optional hardening.
