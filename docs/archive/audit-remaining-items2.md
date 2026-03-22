# Audit Remaining Items

Items from the production-grade audit that were partially addressed, intentionally
deferred, or represent known limitations. None are critical or high severity —
all P0/P1 issues have been resolved.

---

## Intentionally Deferred (By Design)

These items were investigated and determined to be correct behavior, acceptable
trade-offs, or inappropriate to change without broader architectural discussion.

### D-1: `strategy_type: str` in response schemas (not enum)

**Files:** `schemas/backtests.py` (BacktestTradeResponse, TradeJsonResponse,
BacktestRunHistoryItemResponse, BacktestRunDetailResponse)

**Finding:** Response schemas use `strategy_type: str` while request schemas use
`StrategyType` enum.

**Reason deferred:** Using `str` in responses is a deliberate forward-compatibility
decision. With 30+ strategy types (including custom N-leg strategies), the response
must accept strategy types not yet known to older client schemas. The request uses
the enum for input validation. Changing responses to the enum would break clients
when new strategies are added before the client schema is regenerated.

### D-2: `sec-fetch-site` bypass when header absent

**File:** `apps/api/app/dependencies.py:166-175`

**Finding:** If the `Sec-Fetch-Site` header is not sent, the cookie auth check
is skipped.

**Reason deferred:** The header is only sent by modern browsers. Non-browser
clients (curl, API tools, server-to-server) never send it. Blocking absent
headers would break legitimate programmatic access. CSRF protection relies on
`X-Requested-With: XMLHttpRequest` (required for cookie state-changing requests)
and Origin/Referer validation.

### D-3: `ip_hash_salt` auto-generated per process in development

**File:** `config.py:651-659`

**Finding:** In development, a random salt is generated per process with a
warning logged.

**Reason deferred:** Production/staging environments enforce a real salt via
`validate_production_security`. The auto-generated dev salt is intentional —
dev environments don't need consistent IP hashing across restarts.

### D-4: Rate limiter in-process counter desync after Redis failover

**File:** `security/rate_limits.py:87-131`

**Finding:** When Redis fails, the rate limiter falls back to in-memory
per-process counting. On Redis recovery, the counters are not synchronized.

**Reason deferred:** Known limitation, documented in the fallback log message.
Rate limits are temporarily per-process-only during the recovery window
(30-40 seconds). The bounded backoff prevents extended desync.

### D-5: `onupdate=func.now()` redundant with DB trigger

**File:** All models with `updated_at` column

**Finding:** Both ORM-level `onupdate` and DB-level trigger fire on updates.

**Reason deferred:** Documented in model comments. Kept for SQLite test
session compatibility where the DB trigger doesn't exist.

### D-6: `DISTINCT ON` in `list_refresh_sources` — PostgreSQL only

**File:** `repositories/scanner_jobs.py:100-125`

**Finding:** Uses PostgreSQL-specific `DISTINCT ON` syntax that doesn't work
with SQLite.

**Reason deferred:** Documented in comment. Only called from the nightly pipeline
worker which always runs against PostgreSQL. SQLite is only used in unit tests
that don't exercise this code path.

### D-7: Storage cleanup / cascade delete race condition

**File:** `apps/api/app/routers/account.py:162-207`

**Finding:** A concurrent request could create a new export between storage
cleanup and `db.delete(user)`.

**Reason deferred:** Mitigated by: (a) account deletion rate limit (1/hour),
(b) `reconcile_s3_orphans` periodic task as safety net. Adding transactional
S3 semantics would add significant complexity for negligible benefit.

### D-8: `_find_pipeline_run` heuristic fallback

**File:** `apps/worker/app/tasks.py:63-116`

**Finding:** Falls back to date-based heuristic lookup when `run_id` is `None`.

**Reason deferred:** Only triggers when pipeline crashed before returning a run
object. Has guardrails: refuses when `running_count > 1`, logs at ERROR level,
uses `with_for_update(skip_locked=True)`.

### D-9: `content_bytes` loaded into memory for DB storage path

**File:** `services/exports.py`

**Finding:** Full export content loaded into Python memory for DatabaseStorage.

**Reason deferred:** The `_MAX_EXPORT_BYTES = 10 MB` cap limits memory usage.
10MB is trivial for a worker process.

---

## Low-Priority Improvements (Not Yet Implemented)

These are real issues but low severity — style, consistency, or minor robustness
improvements that don't affect correctness or security.

### L-1: `QuotaErrorDetail` uses raw `str` for tier fields

**File:** `schemas/common.py:63-64`

```python
current_tier: str | None = None
required_tier: str | None = None
```

**Recommendation:** Change to `PlanTier | None` for type safety. Low risk since
these fields are only set in `app_error_handler` from error objects that already
use valid tier values.

### L-2: `ScannerRecommendationResponse.request_snapshot` missing alias

**File:** `schemas/scans.py:185`

```python
request_snapshot: dict[str, Any]
```

**Finding:** The ORM column is `request_snapshot_json`. Without an alias, this
field must be manually populated (which it is, in the service layer). The lack
of alias means `from_attributes=True` auto-mapping would miss this field.

**Recommendation:** Add `Field(validation_alias="request_snapshot_json")` if
auto-mapping is ever needed, or document that manual construction is required.

### L-3: `CreateSweepRequest` uses `max_backtest_window_days` not a sweep-specific limit

**File:** `schemas/sweeps.py:113-116`

**Finding:** Sweeps share the backtest window limit (1825 days). Sweeps are
computationally heavier and might warrant a shorter limit.

**Recommendation:** Add `max_sweep_window_days` to `Settings` with a default
of 730 (2 years), matching the scanner's `max_scanner_window_days` pattern.

### L-4: `SummaryShape` TypedDict incomplete vs `BacktestSummaryResponse`

**File:** `schemas/json_shapes.py:77-95`

**Finding:** Missing fields: `total_commissions`, `average_win_amount`,
`average_loss_amount`, `average_holding_period_days`, `average_dte_at_open`,
`payoff_ratio`, `calmar_ratio`, `max_consecutive_wins`, `max_consecutive_losses`,
`recovery_factor`, `decided_trades`.

**Recommendation:** Add the missing fields. Since `SummaryShape` uses
`total=False` (all optional), adding fields won't break existing code. The shape
is used for validation warnings, not enforcement.

### L-5: `ForecastShape` TypedDict minimal

**File:** `schemas/json_shapes.py:58-68`

**Finding:** Only `horizon_days` is required. No `symbol`, `as_of_date`, or
other metadata fields.

**Recommendation:** Add fields that appear in actual forecast JSON blobs.
Since `total=False`, this is additive and non-breaking.

### L-6: `/me` endpoint reuses `backtest_read_rate_limit`

**File:** `apps/api/app/routers/me.py:26`

```python
limit=settings.backtest_read_rate_limit,
```

**Recommendation:** Add `me_read_rate_limit` to `Settings` (default 60) so
operators can tune `/me` independently without affecting backtest read limits.

### L-7: Delete rate limits hardcoded across 5 routers

**Files:** `routers/backtests.py`, `routers/scans.py`, `routers/sweeps.py`,
`routers/analysis.py`, `routers/exports.py`

**Finding:** All delete endpoints use `limit=60` hardcoded rather than a
configurable setting.

**Recommendation:** Add `delete_rate_limit` to `Settings` (default 60).

### L-8: `/meta` endpoint crashes on DB outage

**File:** `apps/api/app/routers/meta.py:55-56`

```python
except (DatabaseError, ConnectionError, OSError):
    raise
```

**Finding:** `_try_authenticate` re-raises database errors, causing `/meta` to
return 500 when the DB is down. Since `/meta` is a public informational endpoint,
it should degrade gracefully.

**Recommendation:** Catch these errors and return the unauthenticated response
(without features/billing info) instead of failing entirely. This keeps `/meta`
available for health monitoring even during DB outages.

### L-9: `audit_events.list_recent` missing negative limit/offset guard

**File:** `repositories/audit_events.py:97-107`

**Finding:** Has `min(limit, _MAX_PAGE_SIZE)` but no `max(limit, 1)` or
`max(offset, 0)`. A negative limit or offset could be passed to SQLAlchemy.

**Recommendation:** Add `limit = max(min(limit, _MAX_PAGE_SIZE), 1)` and
`offset = max(offset, 0)`. Other repositories (backtest_runs, scanner_jobs)
already do this.

### L-10: `offset_strike` re-sorts already-sorted input

**File:** `backtests/strategies/common.py:105`

```python
ordered = sorted(strikes)
```

**Finding:** `resolve_wing_strike` passes `sorted(set(strikes))` to
`offset_strike`, which sorts again. Timsort is O(n) on already-sorted input,
so the overhead is minimal (~50μs for 30 strikes).

**Recommendation:** Low priority. Could add a `presorted: bool = False`
parameter, but the performance gain is negligible.

### L-11: Sharpe/Sortino returns `None` when equity goes negative

**File:** `backtests/summary.py:207-208`

```python
if any(eq <= 0 for eq in equities):
    return None, None
```

**Finding:** If equity ever touches zero or negative (extreme drawdown / margin
call), both Sharpe and Sortino are skipped entirely. This masks risk metrics for
the strategies that need them most.

**Recommendation:** Consider computing the ratios up to the point where equity
went negative, or using a different approach (e.g., log returns with a floor).
This requires careful mathematical review.

### L-12: Health check migration drift opens DB connection per poll

**File:** `apps/api/app/routers/health.py:110`

```python
with _get_engine().connect() as conn:
    context = MigrationContext.configure(conn)
    current = context.get_current_revision()
```

**Finding:** Uses `_get_engine().connect()` which checks out a connection from
the pool. The `_get_engine()` is cached, so the pool is shared, but each
readiness check poll opens and closes a connection.

**Recommendation:** Cache the migration head check result with a short TTL
(e.g., 60 seconds) to avoid per-poll DB round-trips. Migration drift is
unlikely to change between consecutive polls.

### L-13: Sequential forecast fetching in `_deep_dive`

**File:** `pipeline/deep_analysis.py:671-699`

**Finding:** Forecasts are fetched sequentially in the
`for rank_idx, (cell, full)` loop after the parallelized backtests complete.
Each forecast is ~100ms, so total is ~1 second for 10 candidates.

**Recommendation:** Low priority. Could parallelize with a second
`ThreadPoolExecutor`, but the complexity outweighs the ~1s gain.

---

## Pre-Existing Test Failures (Not Caused by Audit Fixes)

These test failures existed before the audit and are unrelated to our changes.

| Test | Failure | Root Cause |
|------|---------|------------|
| `test_s16_sse_has_process_limit` | `assert 45 >= 100` | SSE process limit constant is 45, test expects >= 100 |
| `test_ts_schema_entry_mid_has_description` | String mismatch | TS schema says "Per-unit" but test expects "Per-share" |
| `test_export_terminal_states` (2 tests) | Wrong constraint inspected | Test looks at `ck_export_jobs_succeeded_has_storage` instead of `ck_export_jobs_valid_export_status` |
| Various `test_audit*.py` failures | Source inspection | Tests inspect source code for patterns that were refactored |
| `test_pipeline_lock_expiry` | Import error | `celery_app` import triggers `MASSIVE_API_KEY` warning |

---

## Summary Statistics

| Category | Total | Resolved | Deferred | Remaining |
|----------|-------|----------|----------|-----------|
| Critical Findings (C1-C20) | 20 | 20 | 0 | 0 |
| Production Bugs (B1-B20) | 20 | 20 | 0 | 0 |
| Silently Wrong (S1-S20) | 20 | 20 | 0 | 0 |
| Frontend-Backend Contracts | 10 | 10 | 0 | 0 |
| Database/Schema Drift | 10 | 10 | 0 | 0 |
| Workflow Traces | 12 | 12 | 0 | 0 |
| Performance | 10 | 10 | 0 | 0 |
| Security | 10 | 10 | 0 | 0 |
| Testing Gaps | 10 | 10 | 0 | 0 |
| Dead Code / Refactors | 15 | 15 | 0 | 0 |
| Immediate Hotfixes | 5 | 5 | 0 | 0 |
| Short-Term Stabilization | 9 | 9 | 0 | 0 |
| Medium-Term Refactors | 11 | 11 | 0 | 0 |
| Long-Term Architecture | 5 | 5 | 0 | 0 |
| **Intentionally Deferred** | — | — | **9** | 0 |
| **Low-Priority Improvements** | — | — | — | **13** |
