# Long-Term Architectural Improvements

This document captures architectural improvements identified during the
production-grade audit (March 2026).  Each section describes the current
state, the target architecture, a migration path, and prerequisites.

---

## 1. Event-Sourcing for Billing State

### Current State

Billing state is stored as mutable fields on the `users` table
(`plan_tier`, `subscription_status`, `stripe_subscription_id`, etc.).
Stripe webhooks are processed by `_apply_subscription_to_user()` which
directly mutates these fields.  Out-of-order webhook handling requires
complex guards (period-end comparison, event-created-timestamp ordering,
upgrade detection).

### Target

Replace direct-mutation with an append-only `billing_events` table.
Each Stripe webhook inserts a row.  A projection function computes the
current billing state by replaying events in order.  Benefits:

- **Auditability**: full history of every billing state change
- **Debuggability**: replay to any point in time
- **Simplification**: no out-of-order guards needed (projection handles ordering)
- **Testability**: test the projection function with event sequences

### Migration Path

1. Add `billing_state_events` table (`id`, `user_id`, `event_type`,
   `stripe_event_id`, `event_created_at`, `payload_json`, `created_at`).
2. Dual-write: `_apply_subscription_to_user` inserts into both the new
   table AND updates `users` fields (backward compatible).
3. Add `project_billing_state(user_id)` function that replays events
   and returns the current tier/status.
4. Add reconciliation task that compares projected state vs `users`
   fields and logs discrepancies.
5. Once confident, switch entitlement checks to use the projection.
6. Remove the mutable fields from `users` (or keep as a cache).

### Prerequisites

- The `stripe_events` table already provides partial event history.
  The new table would store the full payload, not just idempotency status.
- The `log_billing_event` function already logs to structlog; the event
  table makes this durable.

---

## 2. OpenTelemetry Distributed Tracing

### Current State

The codebase propagates W3C `traceparent` headers from API → Celery
tasks via dispatch headers.  Structlog binds `traceparent` and
`request_id` to context vars.  No OTel SDK is initialised.

### What Was Implemented

- `backtestforecast/observability/tracing.py`: Optional OTel SDK init
  with auto-instrumentation for FastAPI, SQLAlchemy, Redis, httpx,
  and Celery.  No-op fallback when SDK is not installed.
- `pyproject.toml`: `otel` optional dependency group.
- API lifespan and worker `_on_worker_ready` both call `init_tracing()`.

### To Complete

1. Deploy an OTel collector (e.g. Grafana Alloy, Jaeger, or Tempo).
2. Set `OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317` in the
   environment.
3. Install the optional dependency: `pip install backtestforecast[otel]`.
4. Traces will automatically appear for HTTP requests, DB queries,
   Redis operations, and Celery task execution.

---

## 3. Read Replicas for Heavy Read Endpoints

### Current State

The `database_read_replica_url` config field and `get_readonly_db()`
session factory are already implemented in `db/session.py`.  List,
detail, and compare endpoints can use `Depends(get_readonly_db)`
instead of `Depends(get_db)` to route reads to a replica.

### To Complete

1. Provision a read replica in your database provider.
2. Set `DATABASE_READ_REPLICA_URL` in the environment.
3. Update high-read endpoints to use `get_readonly_db()`:
   - `GET /v1/backtests` (list)
   - `POST /v1/backtests/compare` (read-only despite POST)
   - `GET /v1/scans/{id}/recommendations`
   - `GET /v1/sweeps/{id}/results`
   - `GET /v1/daily-picks`
   - `GET /v1/strategy-catalog`
4. Keep write endpoints on `get_db()` (primary).

---

## 4. Circuit Breaker for Massive API

**Already implemented.** See `integrations/massive_client.py`:
`_massive_circuit = CircuitBreaker(name="massive_api", failure_threshold=5, recovery_timeout=30.0)`.

Health check integration: `/health/ready` reports `massive_api` status
as `ok`, `circuit_open`, `circuit_half_open`, or `unconfigured`.

---

## 5. Export Generation as Separate Microservice

### Current State

Export generation runs in the Celery worker process on the `exports`
queue.  It loads backtest results into memory, generates CSV/PDF, and
writes to S3 or the database.  Memory usage scales with trade count.

### Target

Extract export generation into a standalone service with its own:
- Container image and resource limits (higher memory, lower CPU)
- Dedicated queue (already using the `exports` queue)
- Independent scaling (scale export workers separately from research workers)

### Migration Path

1. Extract `ExportService.execute_export_by_id()` into a standalone
   FastAPI or CLI service that reads from the same database.
2. The Celery task becomes a thin dispatcher that calls the service
   via HTTP or directly imports the function (depending on deployment).
3. Add a `MAX_EXPORT_MEMORY_MB` config to the new service.
4. Use Kubernetes resource limits to enforce the memory boundary.
5. The existing `exports.generate` Celery task signature stays the same
   for backward compatibility.

### Prerequisites

- S3 storage must be configured (database storage is not suitable for
  a separate service that doesn't share the ORM session).
- The export service needs read access to `backtest_runs`,
  `backtest_trades`, and `backtest_equity_points` tables.
