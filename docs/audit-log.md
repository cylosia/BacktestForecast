# Audit Log

## 2026-03-19: Comprehensive Production Audit

### Critical Fixes Applied

1. **Backtest quota off-by-one** — Changed `>` to `>=` in worker quota check (tasks.py)
2. **Heartbeat calls added** — All long-running tasks now call `_update_heartbeat` before execution
3. **Crashed worker redelivery** — `_validate_task_ownership` no longer rejects redelivery for `running` status jobs
4. **TypeScript sweep types updated** — Added `trades_truncated` and `request_snapshot` fields
5. **Poll-outbox disabled** — Removed from beat schedule since OutboxMessage table is never populated
6. **SSE proxy cache header** — Added `cache: "no-store"` to prevent CDN caching of event streams
7. **DB constraints tightened** — Aligned CHECK constraints with Pydantic schema limits
8. **Concurrent sweep limit enforced** — API now checks `max_concurrent_sweeps` before accepting new sweeps
9. **Unknown Stripe statuses preserved** — `normalize_plan_tier` now preserves existing tier for unknown statuses
10. **Market holiday check** — Nightly pipeline now skips market holidays, not just weekends

### Security Improvements

- Added `X-Robots-Tag: noindex, nofollow` to API responses
- Added `Content-Security-Policy` to SSE proxy responses
- Fixed `ip_hash_salt` staging validation to use substring matching
- Added `sslmode=require` guard to `seed_dev_data.py`
- Added `ruff format --check` to CI pipeline

### Monitoring Improvements

- Added `ReaperDurationHigh` and `SweepJobStuckRunning` Grafana alerts
- Documented Redis password rotation procedure in RUNBOOK
- Documented migration rollback procedures in RUNBOOK

### Documentation

- Documented SSE infrastructure status in known-limitations.md
- Documented OutboxMessage scaffolding status in known-limitations.md
- Added sweep TypeScript type drift check to `check_contract_drift.py`

### Tests Added

- `test_quota_boundary.py` — Quota off-by-one, heartbeat placement, redelivery
- `test_concurrent_sweep_limit.py` — Concurrent sweep enforcement
- `test_security_headers.py` — X-Robots-Tag, CSP, HSTS
- `test_cookie_auth_csrf.py` — CSRF protection for cookie auth
- `test_config_production_guards.py` — Production configuration validation
- `test_webhook_error_handling.py` — Webhook error categorization
- `test_export_pdf_pages.py` — PDF page numbering
- `test_sweep_contract.py` — Sweep TypeScript field coverage
- `test_migration_naming.py` — Migration file naming convention

### Remaining Items (Medium/Long-Term)

- Generate sweep TypeScript types from OpenAPI (FIXME #81)
- Move export storage to S3-only (remove `content_bytes` BLOB)
- Implement or remove SSE frontend consumer
- Implement or remove OutboxMessage pattern
- Add percentage-based feature flag rollouts (FIXME #100)
- Separate Redis instances for broker vs cache
