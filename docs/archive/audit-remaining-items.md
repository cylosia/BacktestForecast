# Audit Remaining Items

Items that were partially addressed, deferred, not implemented, or documented as
acceptable risk during the production-grade audit. Organized by priority and
effort level.

---

## Summary

| Category | Count |
|----------|-------|
| Partially addressed (mitigation exists, improvement possible) | 4 |
| Deferred — requires major feature work | 5 |
| Deferred — requires infrastructure changes | 7 |
| Documented as acceptable risk / by design | 4 |
| **Total** | **20** |

---

## Partially Addressed

These have mitigations in place but could be improved further.

### 1. S3Storage.get() loads up to 50MB into memory

**Status:** Mitigated — `stream_object()` context manager available but `get()` unchanged

**File:** `src/backtestforecast/exports/storage.py`

**Current behavior:** `get()` reads up to 50MB into a single `bytes` object
(`_MAX_DOWNLOAD_BYTES = 50 * 1024 * 1024`). A streaming alternative
`stream_object()` was added, but callers of `get()` still buffer the full file.

**Risk:** Memory pressure on API server when serving large export files.

**Recommendation:** Migrate download endpoint (`apps/api/app/routers/exports.py`)
to use `stream_object()` and stream chunks via `StreamingResponse` instead of
materializing the full file in memory.

**Effort:** Small (1-2 hours)

---

### 2. Wheel strategy uses `float` for cash tracking

**Status:** Mitigated — post-hoc Decimal reconciliation at end of backtest

**File:** `src/backtestforecast/backtests/strategies/wheel.py:78`

**Current behavior:** `cash = float(config.account_size)` — all cash accumulation
during the wheel's multi-cycle simulation uses IEEE 754 float arithmetic. A
reconciliation step at the end (lines 493-510) recomputes ending equity from
Decimal-precise trade-level P&L to eliminate drift.

**Risk:** Accumulated float error (~$0.01 per 100 trades) in intra-simulation
equity curve points. The final summary stats use the reconciled Decimal value,
so headline numbers (ROI, Sharpe) are accurate.

**Recommendation:** Convert cash tracking to `Decimal` throughout, matching the
main engine's approach. This is a ~600-line refactor across all wheel code paths.

**Effort:** Medium (4-8 hours)

---

### 3. IV rank regime detection uses realized vol as proxy

**Status:** Documented — field named `iv_rank_proxy`, comments explain limitation

**File:** `src/backtestforecast/pipeline/regime.py:110-119`

**Current behavior:** The pipeline's regime classifier computes `iv_rank_proxy`
from 20-day realized (historical) volatility, not actual implied volatility from
options chains. The `Regime.HIGH_IV` / `LOW_IV` enum values and comments clearly
document this as a proxy.

**Risk:** Regime labels can diverge from actual IV rank during vol risk-premium
compression/expansion. A stock with high realized vol but low implied vol (or
vice versa) would be mislabeled.

**Recommendation:** Integrate IV data from the options chain into the pipeline.
The backtest engine already has `build_estimated_iv_series()` which computes BSM
IV from option quotes — extract this into a shared utility and use it in the
regime classifier for symbols where option data is available.

**Effort:** Large (2-3 days) — requires options data prefetch in the pipeline

---

### 4. SSE ownership check uses separate session (TOCTOU)

**Status:** Acknowledged — low-risk TOCTOU between ownership check and stream

**File:** `apps/api/app/routers/events.py:157-170`

**Current behavior:** `_verify_ownership()` creates a separate synchronous
session via `create_session()`, checks ownership, closes the session, then the
async SSE generator starts. If the resource is deleted between the check and
the stream subscription, the stream connects to an empty Redis Pub/Sub channel.

**Risk:** Minimal — the stream would simply receive no events and eventually
time out. No data leak, no unauthorized access. The resource was owned at the
time of the check.

**Recommendation:** If this becomes a concern, pass the resource ID into the
SSE generator and re-verify ownership on the first heartbeat. Low priority.

**Effort:** Small (1 hour)

---

## Deferred — Major Feature Work

These require multi-sprint engineering effort and new data sources.

### 5. Early assignment modeling for American-style options

**File:** `src/backtestforecast/backtests/engine.py:89-105` (FIXME #98)

**What's missing:** No modeling of early assignment risk. Deep ITM short legs
near ex-dividend dates should trigger forced exercise. Affects covered calls,
iron condors, and any strategy with short American options near ex-div dates.

**Impact:** Backtest P&L may be optimistic for strategies near ex-dividend dates
where early assignment is likely (short call holders exercise to capture the
dividend).

**Implementation plan (from FIXME):**
1. Accept `ex_dividend_dates` set alongside `earnings_dates`
2. In `_mark_position`, check delta > 0.90 near ex-div dates
3. Force-close with `exit_reason="early_assignment"` using intrinsic value
4. Add warning to trade result

**Effort:** Large (1-2 weeks)

---

### 6. Dividend handling for stock-holding strategies

**File:** `src/backtestforecast/backtests/strategies/covered_call.py:5-22` (FIXME #99)

**What's missing:** Strategies that hold shares (covered call, collar, covered
strangle, synthetic put, reverse conversion) do not credit dividends received
during the holding period.

**Impact:** For a stock yielding 3% annually, a 45-day holding period omits
~0.37% of return per trade. This compounds across a multi-year backtest.

**Implementation plan (from FIXME):**
1. Add `DividendGateway` or extend `OptionDataGateway` with dividend schedule
2. Accumulate dividends for ex-dates within [entry, exit)
3. Add `dividends_received` to `TradeResult.detail_json`
4. Report dividends as a separate line item in `build_summary`

**Effort:** Large (1-2 weeks) — requires new market data source for dividend schedules

---

### 7. Put calendar spread support

**File:** `src/backtestforecast/backtests/strategies/calendar.py:31-46` (TODO)

**What's missing:** Only call calendar spreads are supported. Put calendars
(useful for bearish/neutral views) are not available.

**Implementation plan (from TODO):**
1. Add `contract_type: Literal["call", "put"]` field (default "call")
2. Branch on contract_type in `build_position()`
3. Use `naked_put_margin` for put short legs
4. Update `detail_json` and assumption text

**Effort:** Medium (4-8 hours)

---

### 8. Replace ThreadPoolExecutor with ProcessPoolExecutor in genetic optimizer

**File:** `src/backtestforecast/sweeps/genetic.py:240-248` (TODO)

**What's missing:** The genetic algorithm's fitness evaluation uses
`ThreadPoolExecutor`, which is GIL-bound for CPU-intensive backtest computation.
`ProcessPoolExecutor` would parallelize across cores.

**Blocker:** `fitness_fn` is a closure capturing a DB session, backtest engine,
and config — these can't be pickled. Requires refactoring into a top-level
function with explicit serializable arguments.

**Effort:** Medium-Large (1-2 days) — requires fitness function refactoring

---

### 9. Proper IV rank from options data (not realized vol proxy)

**File:** `src/backtestforecast/pipeline/regime.py:110-119`

**What's missing:** The regime classifier uses 20-day realized volatility rank
as a proxy for IV rank. True IV rank requires fetching ATM option chain IV data
for each symbol during the nightly pipeline run.

**Blocker:** The pipeline currently only processes price bars (no option data).
Adding option data fetching would significantly increase pipeline runtime and
API costs.

**Recommendation:** When IV data becomes available in the pipeline, replace
`iv_rank_proxy` with actual `iv_rank` from ATM option chain IV. Consider
caching IV snapshots from the backtest engine's `build_estimated_iv_series()`.

**Effort:** Large (2-3 days)

---

## Deferred — Infrastructure Changes

These require CI/CD, deployment, or test infrastructure work.

### 10. No request coalescing for option contract/quote cache

**File:** `src/backtestforecast/market_data/service.py:128-211`

**What's missing:** `_fetch_bars_coalesced` has inflight deduplication (only one
thread fetches per key while others wait on a `threading.Event`). The option
data methods (`list_contracts`, `get_quote`, `get_snapshot`) lack this — multiple
threads hitting the same cold cache key each make independent API calls.

**Impact:** Under concurrency (multiple scan/sweep workers prefetching the same
symbol), redundant API calls to the Massive data provider.

**Recommendation:** Apply the same `_inflight` dict + `Event` pattern from
`_fetch_bars_coalesced` to `list_contracts` and `get_quote`.

**Effort:** Medium (4-6 hours)

---

### 11. Redis TLS not configured in production

**File:** `docker-compose.prod.yml:35-49`

**Current state:** Redis uses password authentication but no TLS. All services
are on the same Docker `backend` network (not exposed externally). TLS
instructions are documented in comments.

**Recommendation:** Enable TLS when Redis moves to a separate host. Follow the
4-step plan in the docker-compose comments.

**Effort:** Small (1-2 hours) when Redis is externalized

---

### 12. No canary/blue-green deployment in CD pipeline

**File:** `.github/workflows/cd.yml`

**Current state:** Sequential staging → production deployment via
`eval "${DEPLOY_COMMAND}"`. Rollback on smoke test failure (image re-tag).
No traffic splitting, no canary analysis, no gradual rollout.

**Recommendation:** Implement weighted target groups (AWS ALB) or Kubernetes
canary (Argo Rollouts / Flagger) for gradual production rollout with automated
rollback on error rate increase.

**Effort:** Large (1-2 weeks) — requires load balancer/orchestrator setup

---

### 13. No S3 integration tests (moto/localstack)

**Current state:** S3 storage tests use `MagicMock` for the boto3 client. No
moto or localstack-based tests exercise actual S3 operations (put, get, delete,
list, streaming).

**Recommendation:** Add `moto` as a test dependency and create integration tests
for `S3Storage.put()`, `get()`, `stream_object()`, `delete()`, and
`exists()`. Test edge cases: missing keys, oversized files, permission errors.

**Effort:** Small-Medium (4-6 hours)

---

### 14. Worker tests use mock sessions instead of Postgres

**File:** `tests/worker/test_tasks.py:26-31`

**Current state:** All core Celery task tests (`run_backtest`, `run_scan_job`,
`run_sweep`, `generate_export`, `run_deep_analysis`) mock `SessionLocal` and
test orchestration logic only. `FOR UPDATE`, `skip_locked`, advisory locks,
partial unique indexes, and cascade behavior are untested.

**Recommendation:** Add a `tests/integration/test_worker_tasks.py` that uses
the Postgres service container from CI to exercise the full task lifecycle
against a real database.

**Effort:** Medium (1-2 days)

---

### 15. Pipeline race test passes vacuously on SQLite

**File:** `tests/worker/test_pipeline_race.py:65-75`

**Current state:** Test creates a duplicate `NightlyPipelineRun` on SQLite,
which doesn't enforce the partial unique index. The test catches this and
passes with a comment: "SQLite does not enforce partial unique indexes."

**Recommendation:** Move this test to `tests/integration/` and run against
Postgres where the partial unique index is enforced.

**Effort:** Small (1 hour) — just needs to be moved to integration suite

---

### 16. No load tests in CI

**File:** `.github/workflows/ci.yml:71-74`

**Current state:** Load tests exist in `tests/load/` (Locust-based) but are
explicitly documented as manual-only. CI only compile-checks the locustfile.

**Recommendation:** Add a lightweight load test step to the staging deployment
pipeline that runs a short Locust test (e.g., 30 seconds, 10 users) against
the staging URL after deployment.

**Effort:** Small-Medium (2-4 hours) — requires staging URL in CI

---

## Documented as Acceptable Risk / By Design

These were evaluated and determined to be acceptable in their current form.

### 17. Naked option margin uses 25% factor (not FINRA 20%)

**File:** `src/backtestforecast/backtests/margin.py`

**By design:** Uses the 25% factor matching Schwab/TDA broker estimates rather
than the FINRA Rule 4210 minimum of 20%. This is intentionally conservative —
positions are slightly undersized compared to the theoretical minimum, which is
safer for risk management. The choice is documented in the module docstring.

**If needed:** Add a `margin_factor` parameter to `BacktestConfig` so users
can override (0.20 for FINRA minimum, 0.25 for Schwab, etc.).

---

### 18. SSE per-user connection limit multiplied by worker count

**File:** `apps/api/app/routers/events.py:32-38`

**By design:** The per-process SSE connection limit
(`SSE_MAX_CONNECTIONS_PROCESS = 45`) is inherently per-worker. With N uvicorn
workers, the effective limit is N × 45. This is explicitly documented in comments.
Per-user limits are enforced via Redis (cross-worker) when Redis is available.
The in-process fallback is documented as a degraded mode.

---

### 19. Export storage write before DB commit (orphan risk)

**File:** `src/backtestforecast/services/exports.py:250-304`

**By design:** S3 write happens before `session.commit()` because S3 and
Postgres cannot participate in the same transaction. Three mitigations:
1. Immediate cleanup on commit failure
2. CAS-based race detection
3. Daily `reconcile_s3_orphans` Celery beat task at 03:30

The residual risk (process crash between S3 write and commit, AND cleanup fails)
is addressed by the daily reconciliation job.

---

### 20. `populate_by_name=True` + `alias=` serialization for date fields

**File:** `src/backtestforecast/schemas/backtests.py:653-654`

**By design:** `start_date = Field(alias="date_from")` and
`end_date = Field(alias="date_to")` use `alias=` (not `validation_alias=`)
because these ARE the ORM column names AND the desired API output names. The
API contract uses `date_from`/`date_to` in responses, matching the database
columns. Similarly, `ExportJobResponse.run_id = Field(alias="backtest_run_id")`
serializes to `backtest_run_id` which is the ORM column name. This is correct.

---

## Priority Recommendations

### High Priority (significant user/business impact)
1. **Migrate export download to streaming** (#1) — prevents API server OOM
2. **Add S3 integration tests** (#13) — prevents silent storage regressions
3. **Add Postgres worker integration tests** (#14) — validates real DB behavior

### Medium Priority (correctness/accuracy improvements)
4. **Put calendar spread support** (#7) — completes strategy coverage
5. **Request coalescing for option data** (#10) — reduces API costs under concurrency
6. **Convert wheel cash to Decimal** (#2) — eliminates float drift in equity curves

### Lower Priority (long-term quality)
7. **Early assignment modeling** (#5) — improves P&L accuracy near ex-div dates
8. **Dividend handling** (#6) — improves total return accuracy for income strategies
9. **Proper IV rank** (#9) — improves regime classification accuracy
10. **Canary deployment** (#12) — reduces blast radius of bad deploys
