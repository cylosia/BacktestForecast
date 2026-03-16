# BacktestForecast test plan

## Scope
This plan covers all production-critical flows: authenticated access, async backtests, templates, side-by-side comparison, scanner jobs, Stripe subscription sync, async exports, strategy catalog, quota enforcement, and failure handling.

## Automated coverage (55 tests)

### Integration tests (26 tests) — `tests/integration/test_api_critical_flows.py`
- **Auth**: unauthenticated → 401; authenticated → 200 with user state.
- **Async backtests**: POST 202 → inline Celery execution → succeeded; queued without worker; idempotency; multiple strategy types (covered_call, iron_condor, bull_call_debit_spread).
- **Quota enforcement**: Free 6th backtest → `quota_exceeded` (403); Pro unlimited.
- **Templates**: full CRUD (201 → list → get → patch → 204 delete); free limit (3) → `quota_exceeded`; pro higher limit; not found; multi-strategy acceptance.
- **Compare**: Pro 2-run compare → 200 with correct order and limit; free → `feature_locked`; missing run → 404; minimum-two validation → 422; premium 4-run compare → limit 8.
- **Exports**: Pro CSV export → 202 → inline generation → status polling → download with formula sanitization; free → `feature_locked`.
- **Catalog**: GET returns 35 strategies in 9 categories; tier split 6 free + 29 premium; auth required.
- **Scanner**: Pro full flow → 202 → inline execution → recommendations with forecasts; free → `feature_locked`.
- **Stripe webhook**: subscription sync updates plan_tier; duplicate event ignored.
- **`/v1/me` enrichment**: returns features, usage, and quota after a backtest.

### Unit tests (29 tests)
- **Error types** (3): `QuotaExceededError` and `FeatureLockedError` codes, status, and hierarchy.
- **Strategy catalog** (5): all 35 entries present, grouped order, required fields.
- **Template service** (7): CRUD, not-found, free limit 3, pro limit, limit visibility.
- **Entitlements** (4): inactive subscription downgrade, export/forecast access, scanner mode enforcement.
- **Backtests validation** (2): conflicting directional rules rejected, extended indicators accepted.
- **Strategy engine** (3): bull call spread profit, calendar spread exit, wheel multi-cycle.
- **Scanner** (5): forecast analog, ranking recency weighting, ranking breakdown, request normalization, symbol dedup.

### Test infrastructure
- SQLite in-memory for portability (production uses PostgreSQL).
- `FakeExecutionService` stubs market data and engine — returns deterministic results.
- `FakeForecaster` returns predictable analog forecasts.
- `immediate_backtest_execution`, `immediate_export_execution`, and `immediate_scan_execution` fixtures patch Celery to run inline.

## Manual pre-launch checks
1. Run a full paid-plan checkout in Clerk + Stripe test mode.
2. Validate worker and beat containers execute scans end-to-end against staging Massive API.
3. Force a market-data outage and confirm API returns `data_unavailable` (422) without corrupting state.
4. Verify CSV and PDF exports download correctly in the browser for all strategy types.
5. Confirm Clerk route protection works for `/app/*` in the deployed web app.
6. Verify forecast page returns results for a live ticker on the Pro plan.
7. Confirm template picker pre-fills all form fields when applied.
8. Verify comparison page renders correctly for 3+ runs on Premium.

## Release gates
- `pytest -q` passes.
- `python -m compileall py apps tests scripts` passes.
- `pnpm lint:web` and `pnpm typecheck:web` pass in CI.
- Staging smoke test validates `/health/live`, `/health/ready`, one backtest, one scanner job, one export, one forecast.
