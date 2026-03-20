# Audit Remaining Items

This document captures everything from the production-grade audit that was
**not fully completed** — items that are partially addressed, deferred,
documented-only, or remain as recommendations for future work.

Last updated: 2026-03-20

---

## Table of Contents

1. [Partially Addressed](#1-partially-addressed)
2. [Deferred Refactors](#2-deferred-refactors)
3. [Frontend Gaps Not Yet Consumed](#3-frontend-gaps-not-yet-consumed)
4. [Infrastructure / Ops Recommendations](#4-infrastructure--ops-recommendations)
5. [Monitoring & Alerting Gaps](#5-monitoring--alerting-gaps)
6. [CI/CD Pipeline Recommendations](#6-cicd-pipeline-recommendations)
7. [Documentation Gaps](#7-documentation-gaps)
8. [Known Acceptable Risks](#8-known-acceptable-risks)

---

## 1. Partially Addressed

### 1.1 tasks.py Split (Critical #8)

**Status:** Foundation laid, migration not complete.

**What was done:**
- `BaseTaskWithDLQ` and DLQ infrastructure extracted to `task_base.py`
- `task_helpers.py` holds `commit_then_publish`, `mark_job_failed`, `handle_task_app_error`
- `maintenance_tasks.py` created as a re-export module
- `celery_app.py` include list updated

**What remains:**
- Actual task function definitions (18 tasks, ~2000 lines) still live in `tasks.py`
- `maintenance_tasks.py` re-exports from `tasks.py` instead of defining tasks directly
- Domain tasks (backtests, exports, scans, sweeps, analysis, pipeline) should each get their own module

**Recommended next step:**
Move one maintenance task at a time from `tasks.py` to `maintenance_tasks.py`.
Verify with `celery inspect registered` after each move.
The re-export pattern makes this safe — both locations resolve to the same function.

**Risk if not done:** Low. The code works correctly; this is purely a maintainability improvement.

---

### 1.2 Cursor Pagination — Frontend Not Yet Using It

**Status:** Backend fully implemented, frontend not consuming `next_cursor`.

**What was done:**
- All 6 list API endpoints accept `cursor` query parameter
- All list response schemas include `next_cursor` field
- `BacktestRunListResponse` in `schema.d.ts` has `next_cursor`
- Repositories and services support `cursor_before` keyset pagination

**What remains:**
- No frontend component actually reads `next_cursor` or passes `cursor` to API calls
- `apps/web/lib/api/server.ts` functions (`getBacktestHistory`, `getScannerJobs`, etc.) still use `offset` only
- Infinite scroll or "Load More" UI needs to be built using the cursor

**Recommended next step:**
Update `getBacktestHistory()` in `server.ts` to accept an optional `cursor` parameter.
Add a `useInfiniteList` hook that uses `next_cursor` for efficient pagination.

**Risk if not done:** Medium. Offset pagination works but degrades at high page numbers.

---

### 1.3 Frontend `fieldErrors` — Available But Not Rendered

**Status:** Type system wired, no component consumes it.

**What was done:**
- `ApiError.fieldErrors` carries per-field validation errors from 422 responses
- `ValidationFieldError` type with `loc`, `msg`, `type` fields defined
- `handleKnownStatus` and `parseApiError` propagate `details` → `fieldErrors`

**What remains:**
- No form component highlights specific fields based on `fieldErrors`
- Forms show the server's top-level `message` but not which field caused the error

**Recommended next step:**
In `backtest-form.tsx`, after catching an `ApiError` with status 422, map
`error.fieldErrors` to the `BacktestFormErrors` state to highlight the offending fields.

**Risk if not done:** Low. Users see the error message; they just don't see which field to fix.

---

### 1.4 `ExportJobListResponse` — Exported But Not Imported

**Status:** Type exported from api-client package, not imported by any component.

**What was done:**
- Added `ExportJobListResponse` to `packages/api-client/src/index.ts`

**What remains:**
- No frontend component imports or uses `ExportJobListResponse`
- The exports list page likely uses an inline type or `any`

**Risk if not done:** None for runtime. Type safety gap only.

---

### 1.5 Read Replica Routing — Config Exists, No Endpoints Use It

**Status:** Infrastructure fully built, not wired to any router.

**What was done:**
- `database_read_replica_url` config field
- `_get_readonly_engine()`, `_get_readonly_session_factory()`, `get_readonly_db()`
- Falls back to primary when not configured

**What remains:**
- No router uses `Depends(get_readonly_db)` — all use `Depends(get_db)`
- High-read endpoints should be migrated: `GET /v1/backtests`, `GET /v1/daily-picks`,
  `GET /v1/strategy-catalog`, `POST /v1/backtests/compare`

**Recommended next step:**
Change `list_backtests` in `routers/backtests.py` to use `get_readonly_db` as a pilot.
Monitor query latency difference.

**Risk if not done:** None until read load requires a replica.

---

## 2. Deferred Refactors

### 2.1 Centralize All Redis Connections

**Status:** Partially done.

**What was done:**
- `create_cache_redis()` helper in `utils/__init__.py`
- 6 ad-hoc connections in `tasks.py` migrated to the helper

**What remains:**
- `celery_app.py` heartbeat/holidays/shutdown: 3 connections still use `Redis.from_url()` directly
- `health.py` broker ping: creates ad-hoc `Redis.from_url(redis_url)`
- `events.py` SSE Redis: has its own singleton pattern
- `rate_limits.py`: has its own singleton pattern

**Recommendation:**
The remaining ad-hoc connections are in startup/shutdown code where using the
helper would add an import that may not be available yet. Leave as-is unless
they cause maintenance issues.

---

### 2.2 Migration History Squash

**Status:** Documented procedure, not executed.

**What was done:**
- Squash procedure documented in `alembic/versions/README.md`
- Step-by-step commands for creating a consolidated baseline

**What remains:**
- Actually running the squash (requires all environments at the same revision)
- 47 migration files remain on disk

**Recommendation:**
Execute the squash after the next production deployment stabilizes.
All environments must be at revision `20260319_0044` before squashing.

---

### 2.3 OpenAPI Snapshot Regeneration

**Status:** Backend schemas updated, snapshot stale.

**What was done:**
- `next_cursor` added to `BacktestRunListResponse` in `schema.d.ts` manually
- Backend Pydantic schemas all have `next_cursor` fields

**What remains:**
- The full OpenAPI snapshot should be regenerated from the running API:
  ```bash
  python scripts/export_openapi.py > openapi.snapshot.json
  npx openapi-typescript openapi.snapshot.json -o packages/api-client/src/schema.d.ts
  ```
- This would also pick up any other schema changes not yet reflected in TypeScript

**Risk if not done:** Medium. TypeScript types may drift from actual API responses.

---

## 3. Frontend Gaps Not Yet Consumed

| Feature | Backend Status | Frontend Status | Gap |
|---------|---------------|-----------------|-----|
| `next_cursor` on list responses | All 6 endpoints return it | No component reads it | Pagination still uses offset |
| `fieldErrors` on 422 responses | `ApiError.fieldErrors` populated | No form renders per-field errors | Forms show generic message only |
| `requiredTier` on 403 responses | `ApiError.requiredTier` populated | `UpgradePrompt` renders it | **Fully consumed** |
| `ExportJobListResponse` type | Exported from api-client | Not imported by any component | Type gap only |
| `SweepJobListResponse.next_cursor` | Backend has it | Manual TS type missing `next_cursor` | Manual type needs update |

---

## 4. Infrastructure / Ops Recommendations

### Not Implemented

| # | Recommendation | Priority | Effort |
|---|---------------|----------|--------|
| 1 | Deploy an OTel collector and set `OTEL_EXPORTER_OTLP_ENDPOINT` | High | Medium — infra change |
| 2 | Provision a read replica and set `DATABASE_READ_REPLICA_URL` | Medium | Medium — infra change |
| 3 | Add `idle_in_transaction_timeout` to PostgreSQL config | Medium | Low — 1 line in pg config |
| 4 | Add `log_min_duration_statement = 1000` to PostgreSQL config | Medium | Low — 1 line in pg config |
| 5 | Set up TLS for Redis connections (`rediss://`) in production | Low | Medium — cert management |
| 6 | Add Dockerfile health check for worker container | Low | Low — add `HEALTHCHECK` directive |
| 7 | Add connection draining on API shutdown | Low | Medium — SIGTERM handler |

---

## 5. Monitoring & Alerting Gaps

### Metrics That Exist But Need Alerting Rules

| Metric | Suggested Alert |
|--------|----------------|
| `dlq_depth` | Alert if > 0 for > 5 minutes |
| `db_pool_exhaustion_warning` | Alert if = 1 for > 2 minutes |
| `jobs_stuck_running{model=*}` | Alert if > 0 for > 30 minutes |
| `redis_rate_limit_fallback_total` | Alert on any increment (Redis is down) |
| `billing_unknown_subscription_status_total` | Alert on any increment (new Stripe status) |
| `http_response_size_bytes` (p99) | Alert if > 5 MB (response size regression) |
| `bff_outbox_recovered_total` | Alert if > 10/hour (dispatch reliability issue) |
| `circuit_breaker_state{service="massive_api"}` | Alert if = 2 (OPEN) for > 5 minutes |

### Metrics Not Yet Created

| Metric | Purpose |
|--------|---------|
| `s3_operation_duration_seconds` | Track S3 put/get/delete latency |
| `statement_timeout_total` | Count PostgreSQL `QueryCanceled` per endpoint |
| `subscription_reconciliation_actions_total` | Track billing reconciliation outcomes |

---

## 6. CI/CD Pipeline Recommendations

### Not Implemented

| # | Check | Purpose | Effort |
|---|-------|---------|--------|
| 1 | `alembic check` in CI | Detect model/migration drift before merge | Low |
| 2 | `python scripts/check_openapi_drift.py` in CI | Detect OpenAPI snapshot staleness | Low |
| 3 | `python scripts/check_contract_drift.py` in CI | Detect TS type drift from backend | Low |
| 4 | `pip-audit` or `safety check` in CI | Detect dependency vulnerabilities | Low |
| 5 | Container image scanning (Trivy/Grype) | Detect OS-level vulnerabilities | Medium |
| 6 | SBOM generation | Supply chain compliance | Low |
| 7 | Migration safety check | Reject `ALTER TABLE ADD COLUMN NOT NULL` without `DEFAULT` | Medium |
| 8 | `mypy --strict` in CI | Already configured in pyproject.toml but may not run in CI | Low |
| 9 | Frontend `tsc --noEmit` in CI | Type-check TypeScript changes | Low |

---

## 7. Documentation Gaps

| Document | Status |
|----------|--------|
| `docs/architecture/future-improvements.md` | **Created** — covers event-sourcing, OTel, read replicas, export microservice |
| `alembic/versions/README.md` | **Updated** — branch diagram, squash procedure |
| Runbook for DLQ investigation | Not created |
| Runbook for billing reconciliation | Not created |
| Runbook for subscription state debugging | Not created |
| API rate limit documentation for consumers | Not created |
| Environment variable reference | Partially covered by `config.py` docstrings |

---

## 8. Known Acceptable Risks

These items were investigated, confirmed as design decisions (not bugs),
and documented for future reference.

| Item | Assessment |
|------|-----------|
| `onupdate=func.now()` redundant with DB trigger | Intentional — ORM convenience + trigger safety net. No conflict. |
| `lazy="raise"` on all relationships | Intentional N+1 prevention. All code paths use eager loading. |
| `expire_on_commit=True` triggers lazy loads after commit | Default SQLAlchemy behavior. All worker paths call `session.refresh()`. ~1ms overhead per access. |
| `pool_pre_ping=True` adds SELECT 1 per checkout | ~1ms overhead. Prevents handing dead connections to the application. |
| `_D_CACHE` hand-rolled Decimal cache | 68 call sites in hot engine loop. Bounded at 4096 entries. Justified. |
| Rate limit per-process memory fallback | With `fail_closed=true` and halved limits, worst case is ~2× with 4 workers. |
| CRC32 non-uniform distribution in feature flags | Max bias 0.000002%. Negligible. |
| `ScannerJob._validate_evaluated_count` ORM-only | DB CHECK provides floor. Direct SQL paths are internal. |
| `max_tasks_per_child=200` kills after current task | Celery waits for the current task to finish. Not a mid-execution kill. |
| Sweep mode CHECK constraint requires migration | Intentional correctness guard. Migration cost is the price of data integrity. |
| JWKS signing keys cached for 1 hour | Keys rotate weekly. 1-hour cache safely survives Clerk outages. |

---

## Summary

| Category | Count |
|----------|-------|
| Partially addressed (code works, UX/frontend incomplete) | 5 |
| Deferred refactors (documented, not executed) | 3 |
| Frontend features built but not consumed | 4 |
| Infrastructure/ops recommendations | 7 |
| Monitoring alerting rules needed | 8 |
| CI/CD checks to add | 9 |
| Documentation gaps | 5 |
| Known acceptable risks (no action needed) | 11 |
