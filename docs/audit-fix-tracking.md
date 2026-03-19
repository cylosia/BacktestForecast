# Audit Fix Tracking — March 2026

## Status Legend
- [x] Implemented
- [ ] Pending (requires design/larger refactor)

## P0 — Critical (Deployed)
- [x] Fix 1: Define `_commit_then_publish` in tasks.py
- [x] Fix 2: Write tests for `_commit_then_publish` paths
- [x] Fix 3: Add `ExternalServiceError` retry to `run_scan_job`
- [x] Fix 4: Test scan retry on external error

## P1 — High Priority (This Week)
- [x] Fix 5: Replace module-level `settings` with `get_settings()` in main.py
- [x] Fix 6: Add `FOR UPDATE` to `reconcile_subscriptions` query
- [x] Fix 7: Convert `_resolve_position_size` to Decimal
- [x] Fix 8: Add `holding_period_trading_days` field
- [x] Fix 9-10: Tests for commit_then_publish, scan retry
- [x] Fix 11: Test position sizing with Decimal
- [x] Fix 12-13: Document holding_period_days, add migration
- [x] Fix 14: Webhook payload size (already 512KB)
- [x] Fix 15-16: Remove dead code
- [x] Fix 17: Test _entry_underlying_close

## P2 — This Sprint
- [x] Fix 18: Verify ExternalServiceError retry in run_deep_analysis
- [x] Fix 19-20: Document entry_mid/exit_mid and holding_period_days in schema
- [x] Fix 21-22: Create task_helpers.py module, shared error handler
- [x] Fix 23: Postgres CHECK constraint tests
- [x] Fix 24-26: JSON shape validation for summary_json
- [x] Fix 27: Audit event archival logging before deletion
- [x] Fix 28-29: Option cache staleness metric and config
- [x] Fix 30: DLQ Redis (already uses backend client)
- [x] Fix 31: Require admin_token in production
- [x] Fix 32: Webhook payload size (already in place)
- [x] Fix 33-34: Stripe event error marking tests
- [x] Fix 35: Strict evaluated_candidate_count validation
- [x] Fix 37: Strategy type validation (already in engine)
- [x] Fix 40-42: Export size limit tests and docs
- [x] Fix 43-44: Pipeline run_id tracking improvement
- [x] Fix 46: sec-fetch-site rejection (upgraded from warning)
- [x] Fix 47: CSV sanitization extended tests
- [x] Fix 48: CancelledError middleware logging
- [x] Fix 50: Settings invalidation callback limit (already present)

## P3 — Next Sprint
- [x] Fix 51-52: Pipeline-Scanner FK migration
- [x] Fix 53-54: holding_period_trading_days migration
- [x] Fix 57: Consolidate CONTRACT_MULTIPLIER constant
- [x] Fix 58: Document _mark_position mutation
- [x] Fix 59-60: _D() cache for common values
- [x] Fix 61: max_reconciliation_users config
- [x] Fix 63: audit_cleanup_retention_days config
- [x] Fix 64: cleanup_daily_recommendations dry-run mode
- [x] Fix 65: Settings invalidation metrics
- [x] Fix 66-67: Document CORS PUT exclusion
- [x] Fix 69: cleanup_outbox metrics
- [x] Fix 70: poll_outbox Sentry alerting

## P4 — Medium-Term
- [x] Fix 71-72: Job status state machine
- [x] Fix 73: Job status enum in OpenAPI (already present)
- [x] Fix 76-77: Early assignment risk tracking test
- [x] Fix 78: Multi-underlying documentation
- [x] Fix 82-83: Option cache warn age config
- [x] Fix 88-89: Schema field descriptions

## P5 — Long-Term (Design Required)
- [ ] Fix 91: Replace Celery with task-specific queues
- [ ] Fix 92: Structured result storage table
- [ ] Fix 93: Formal SLA metrics
- [ ] Fix 94: Chaos testing framework
- [ ] Fix 95: Canary deployment pipeline
- [ ] Fix 96: Blue-green API deployment
- [ ] Fix 97: Read replicas for reporting
- [ ] Fix 98: Event sourcing for billing
- [ ] Fix 99: GraphQL API
- [ ] Fix 100: API versioning beyond v1
