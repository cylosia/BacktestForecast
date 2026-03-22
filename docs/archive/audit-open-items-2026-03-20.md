# Audit Open Items — 2026-03-20

Items that are partially addressed, deferred, not implemented, or documented as
known limitations from the exhaustive production-grade audit. Items that were
completely fixed are omitted.

---

## Summary

| Category | Count |
|----------|-------|
| Partially addressed (mitigation exists, root cause remains) | 4 |
| Deferred — requires dedicated engineering effort | 5 |
| Deferred — requires infrastructure / CI changes | 7 |
| Known limitations (documented, accepted by design) | 8 |
| Forward-looking recommendations (not bugs) | 6 |
| **Total open items** | **30** |

**None of these items are blocking deployment or causing active production
incidents.** All partially addressed items have working mitigations. All
deferred items are planned improvements. All known limitations are documented
design choices.

---

## 1. Partially Addressed

These have mitigations in place but the root cause is not fully eliminated.

### PA-1: Scan candidates accumulated in memory before DB flush

| Field | Detail |
|-------|--------|
| **Severity** | Medium |
| **Category** | Performance / Memory |
| **File** | `src/backtestforecast/services/scans.py:299` |
| **Current mitigation** | Cap reduced from 2000 to 1000. Low-ranked candidates have heavy fields (trades, equity_curve) cleared during periodic trimming. |
| **What remains** | True batch-flushing — writing candidates to the DB every ~100 instead of accumulating all in memory. At ~50 KB per candidate, 1000 candidates is ~50 MB peak. |
| **Production risk** | OOM kill on memory-constrained workers processing large scan universes. Mitigated by the 1000-candidate cap. |
| **Recommended fix** | Refactor `_execute_scan` to maintain a bounded heap of top-K candidates and flush completed batches to a staging table during execution. |
| **Effort** | 2–3 days |

### PA-2: Wheel engine uses float arithmetic (Decimal reconciliation mitigates)

| Field | Detail |
|-------|--------|
| **Severity** | Low |
| **Category** | Correctness / Precision |
| **File** | `src/backtestforecast/backtests/strategies/wheel.py:78` |
| **Current mitigation** | Cash accumulator uses `float`. A reconciliation step (lines 463–478) corrects final ending equity by summing trade-level `Decimal` P&L. Drift > $0.02 triggers a WARNING log. |
| **What remains** | Full conversion to `Decimal` arithmetic matching the main engine. Intermediate equity curve points may have sub-cent inaccuracies. |
| **Production risk** | Equity curve charts show slightly different intermediate values than a Decimal engine would produce. Final summary stats (ROI, Sharpe, CAGR) are correct due to reconciliation. |
| **Recommended fix** | Convert `cash`, `peak_equity`, `option_value`, `shares_value` to `Decimal`. Wrap all interacting float values in `Decimal(str(...))`. |
| **Effort** | 1–2 days + regression testing |

### PA-3: Billing audit trail drops events on DB failure

| Field | Detail |
|-------|--------|
| **Severity** | Medium |
| **Category** | Audit / Compliance |
| **File** | `src/backtestforecast/billing/events.py:103-109` |
| **Current mitigation** | Exception logged at ERROR level with `exc_info=True`. The billing state change itself still succeeds (correct trade-off — billing should not fail because auditing fails). |
| **What remains** | A fallback persistence mechanism so dropped audit events can be replayed after the DB recovers. |
| **Production risk** | Transient DB failures during webhook processing create invisible gaps in the billing audit trail, complicating dispute resolution. |
| **Recommended fix** | On audit write failure, append to a fallback file or Redis list. Add a periodic task to drain the fallback into the DB. |
| **Effort** | 1 day |

### PA-4: Reconciliation FOR UPDATE locks released per-user

| Field | Detail |
|-------|--------|
| **Severity** | Low |
| **Category** | Concurrency |
| **File** | `src/backtestforecast/services/billing.py:530-546` |
| **Current mitigation** | `session.commit()` per user releases all FOR UPDATE locks. Documented comment (lines 530–538) explains acceptability: idempotent operation, `skip_locked=True` prevents initial contention, duplicate processing only wastes Stripe API calls. |
| **What remains** | Per-user transaction isolation. After committing user 1, users 2–100 lose their locks and a concurrent reconciliation could re-process them. |
| **Production risk** | Wasted Stripe API calls and duplicate audit log events during overlapping reconciliation runs. Zero data corruption risk. |
| **Recommended fix** | Process each user in a separate session with its own FOR UPDATE lock. |
| **Effort** | 0.5 days |

---

## 2. Deferred — Engineering Effort

These require multi-day focused work and should be planned as separate stories.

### DE-1: Split `tasks.py` into per-domain modules

| Field | Detail |
|-------|--------|
| **Category** | Maintainability |
| **File** | `apps/worker/app/tasks.py` (~2300 lines) |
| **Current state** | `task_helpers.py` and `task_base.py` extracted as shared infrastructure. All task definitions remain in one file. |
| **Target** | `backtest_tasks.py`, `export_tasks.py`, `scan_tasks.py`, `sweep_tasks.py`, `analysis_tasks.py`, `maintenance_tasks.py`, `pipeline_tasks.py` |
| **Blockers** | Celery task names are strings in beat schedule, outbox maps, and dispatch calls. All must be verified after the split. |
| **Effort** | 2–3 days |

### DE-2: Split `BillingService` into focused classes

| Field | Detail |
|-------|--------|
| **Category** | Maintainability |
| **File** | `src/backtestforecast/services/billing.py` (~940 lines) |
| **Target** | `WebhookHandler`, `CheckoutService`, `ReconciliationService`, `StripeCustomerService`. Shared: `_get_stripe_client()`, session management. |
| **Effort** | 1–2 days |

### DE-3: Extract `ScanExecutor` from `scans.py`

| Field | Detail |
|-------|--------|
| **Category** | Maintainability |
| **File** | `src/backtestforecast/services/scans.py` (~1100 lines) |
| **Target** | `ScanExecutor` class with session, payload, and policy as constructor args. `ScanService.run_job()` becomes a thin orchestrator. |
| **Effort** | 1–2 days |

### DE-4: Early assignment modeling for American options

| Field | Detail |
|-------|--------|
| **Category** | Financial Correctness |
| **File** | `src/backtestforecast/backtests/engine.py:89-106` (FIXME #98) |
| **Impact** | Overstates returns for covered call / naked call strategies on dividend-paying stocks near ex-dividend dates. |
| **Plan** | (1) Accept `ex_dividend_dates`, (2) check delta > 0.90 near ex-div, (3) force-close with `exit_reason="early_assignment"` using intrinsic value, (4) add warning. |
| **Effort** | 3–5 days |

### DE-5: Batch scan candidate flushing with streaming progress

| Field | Detail |
|-------|--------|
| **Category** | Performance / Scalability |
| **File** | `src/backtestforecast/services/scans.py:295-416` |
| **Approach** | Bounded heap of top-K candidates. Flush batches to staging table every 100 evaluations. Final commit moves top-K to `scanner_recommendations`. Memory bounded at O(K). |
| **Effort** | 2–3 days |

---

## 3. Deferred — Infrastructure / CI

These require test infrastructure, deployment pipeline, or external service
configuration work.

### IN-1: Postgres-level concurrent tests for race conditions

| Field | Detail |
|-------|--------|
| **Category** | Testing |
| **What's needed** | Tests exercising `FOR UPDATE`, `pg_advisory_xact_lock`, `SKIP LOCKED`, and concurrent `INSERT ... ON CONFLICT`. SQLite doesn't support these. |
| **Affected code** | Billing customer creation race, scan/sweep idempotency, reconciliation lock contention. |
| **Approach** | `testcontainers` or `docker-compose` for PostgreSQL. Separate CI job with `--postgres` marker. |
| **Effort** | 1–2 days infra + 1 day per test |

### IN-2: No request coalescing for option contract/quote cache

| Field | Detail |
|-------|--------|
| **Category** | Performance |
| **File** | `src/backtestforecast/market_data/service.py:128-211` |
| **Current state** | `_fetch_bars_coalesced` has inflight deduplication. Option data methods (`list_contracts`, `get_quote`) lack this. |
| **Impact** | Redundant Massive API calls under concurrency. |
| **Approach** | Apply the same `_inflight` dict + `Event` pattern to option data methods. |
| **Effort** | 4–6 hours |

### IN-3: Redis TLS not configured in production docker-compose

| Field | Detail |
|-------|--------|
| **Category** | Security |
| **File** | `docker-compose.prod.yml` |
| **Current state** | Password auth, no TLS. Services on same Docker network. TLS instructions in comments. |
| **When needed** | When Redis moves to a separate host. |
| **Effort** | 1–2 hours (when externalized) |

### IN-4: No canary/blue-green deployment in CD pipeline

| Field | Detail |
|-------|--------|
| **Category** | Deployment Safety |
| **File** | `.github/workflows/cd.yml` |
| **Current state** | Sequential staging → production. Rollback on smoke test failure. No traffic splitting. |
| **Approach** | Weighted target groups (AWS ALB) or Kubernetes canary (Argo Rollouts). |
| **Effort** | 1–2 weeks |

### IN-5: No S3 integration tests (moto/localstack)

| Field | Detail |
|-------|--------|
| **Category** | Testing |
| **Current state** | S3 tests use `MagicMock`. No real S3 API exercised. |
| **Approach** | Add `moto` as test dependency. Test `put()`, `get()`, `stream_object()`, `delete()`, edge cases. |
| **Effort** | 4–6 hours |

### IN-6: Worker tests use mock sessions instead of Postgres

| Field | Detail |
|-------|--------|
| **Category** | Testing |
| **Current state** | Core Celery task tests mock `SessionLocal`. `FOR UPDATE`, advisory locks, cascade behavior untested against real DB. |
| **Approach** | `tests/integration/test_worker_tasks.py` using Postgres container from CI. |
| **Effort** | 1–2 days |

### IN-7: No load tests in CI pipeline

| Field | Detail |
|-------|--------|
| **Category** | Testing |
| **Current state** | Locust tests exist in `tests/load/` but are manual-only. CI only compile-checks the locustfile. |
| **Approach** | Lightweight Locust step in staging deployment pipeline (30 seconds, 10 users). |
| **Effort** | 2–4 hours |

---

## 4. Known Limitations (Documented, Accepted)

Design trade-offs or approximations that are understood, documented in the
code, and accepted as appropriate.

### KL-1: Sortino ratio uses partial downside deviation variant

- **File:** `src/backtestforecast/backtests/summary.py:185-195`
- **Behavior:** Denominator uses N-1 (all observations), not count of negative returns. Sortino & Price (1994) variant. Documented in docstring.
- **Action:** Consider frontend tooltip explaining the variant.

### KL-2: Calendar spread margin approximation

- **File:** `src/backtestforecast/backtests/strategies/calendar.py:80-93`
- **Behavior:** `max(full_margin - long_leg_value, net_debit)`. Credit calendars may understate margin. Comment documents limitation.
- **Action:** None required. Consider broker-specific margin modes as future config.

### KL-3: Double diagonal 50% margin heuristic

- **File:** `src/backtestforecast/backtests/strategies/diagonal.py:29`
- **Behavior:** `DOUBLE_DIAGONAL_MARGIN_FACTOR = 0.50`. Empirical, no regulatory basis.
- **Action:** Consider making configurable via `BacktestConfig`.

### KL-4: Forecast analog uses L1 (Manhattan) distance

- **File:** `src/backtestforecast/forecasts/analog.py:232-233`
- **Behavior:** L1 distance for analog selection. Less outlier-sensitive than L2 (appropriate for fat tails).
- **Action:** None. Consider L2 as configurable option if accuracy analysis warrants.

### KL-5: Trading-day approximation in forecast horizon

- **File:** `src/backtestforecast/forecasts/analog.py:289-296`
- **Behavior:** `_calendar_to_trading_days` uses `round(days * 5/7) - round(days * 9/365)`. ~1 day error at 90-day horizon.
- **Action:** None. Negligible for statistical forecasts.

### KL-6: `json_shapes` non-strict default

- **File:** `src/backtestforecast/schemas/json_shapes.py:108`
- **Behavior:** `strict=False` returns `False` with warning log. `strict=True` raises `ValueError`.
- **Action:** New critical code paths should use `strict=True`.

### KL-7: Health endpoint per-process rate limiting (by design)

- **File:** `apps/api/app/routers/health.py:126-136`
- **Behavior:** `deque`-based in-process rate limiting. Effective limit is N x RPM across workers.
- **Rationale:** Health probes must not depend on Redis to avoid circular failure modes.

### KL-8: IV rank regime uses realized vol proxy

- **File:** `src/backtestforecast/pipeline/regime.py:110-119`
- **Behavior:** 20-day realized vol rank as proxy for IV rank. Field named `iv_rank_proxy`.
- **Action:** Integrate IV data from options chain when available in pipeline.

---

## 5. Forward-Looking Recommendations

Not bugs or regressions — improvements that would materially enhance quality.

### REC-1: `last_known_good_tier` timeout for unknown Stripe statuses

| Priority | Medium |
|----------|--------|
| **File** | `src/backtestforecast/billing/entitlements.py:235-248` |
| **Current** | Unknown statuses preserve current `plan_tier`, log ERROR, increment metric. |
| **Recommendation** | Time-limited cache. If unknown status persists > 24 hours without operator action, auto-downgrade to FREE with escalated alert. |

### REC-2: OpenTelemetry tracing for critical paths

| Priority | Medium |
|----------|--------|
| **Current** | Tracing context propagated via `traceparent`. Sentry configured. |
| **Recommendation** | Add spans for backtest engine execution, Stripe API calls, Massive API calls, scan candidate loop. Enables flamegraph latency analysis. |

### REC-3: Database query performance monitoring

| Priority | Low |
|----------|--------|
| **File** | `src/backtestforecast/db/session.py` |
| **Recommendation** | SQLAlchemy event listeners for queries > 500 ms. Complement with `pg_stat_statements`. |

### REC-4: `response_model` on all API endpoints

| Priority | Low |
|----------|--------|
| **Current** | Most endpoints have `response_model`. Root `GET /` does not. |
| **Recommendation** | Add Pydantic models to remaining endpoints. Run `check_openapi_drift.py` in CI. |

### REC-5: Prometheus cardinality monitoring

| Priority | Low |
|----------|--------|
| **Current** | Stripe event types collapsed to `_KNOWN_STRIPE_EVENTS` set; unknowns → `"other"`. |
| **Recommendation** | Periodic health check counting distinct label values per metric. Alert on cardinality threshold breach. |

### REC-6: Structured error codes consistency audit

| Priority | Low |
|----------|--------|
| **Recommendation** | Ensure every `AppError` includes machine-readable `error_code` for frontend i18n. Audit all `raise AppError(code=..., message=...)` for naming consistency. |

---

## Priority Matrix

### Do First (high impact, moderate effort)

| # | Item | Effort |
|---|------|--------|
| 1 | **PA-1** Scan candidate batch-flushing | 2–3 days |
| 2 | **IN-5** S3 integration tests with moto | 4–6 hours |
| 3 | **IN-6** Postgres worker integration tests | 1–2 days |
| 4 | **PA-3** Billing audit fallback persistence | 1 day |

### Do Next (medium impact, contained effort)

| # | Item | Effort |
|---|------|--------|
| 5 | **DE-2** Split BillingService | 1–2 days |
| 6 | **IN-2** Option data request coalescing | 4–6 hours |
| 7 | **PA-2** Wheel engine Decimal conversion | 1–2 days |
| 8 | **DE-3** Extract ScanExecutor class | 1–2 days |

### Plan for Later (large effort or low urgency)

| # | Item | Effort |
|---|------|--------|
| 9 | **DE-1** Split tasks.py | 2–3 days |
| 10 | **DE-4** Early assignment modeling | 3–5 days |
| 11 | **IN-1** Postgres concurrent tests | 2–3 days |
| 12 | **IN-4** Canary deployment | 1–2 weeks |
