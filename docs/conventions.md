# Shared Conventions

## Architecture
- Modular monolith
- One Python codebase shared by API and worker
- Long-running work goes through Celery (backtests, scans, exports)
- Postgres is the durable source of truth
- Frontend must not become a second business-rules engine
- Backend entitlements are authoritative for all plan enforcement

## Product invariants
- Runs are immutable after submission
- Templates are editable
- Manual backtests are async (queued → running → succeeded/failed)
- Daily-bar execution model only (not intraday)
- Strategy catalog is the canonical source for strategy metadata
- Error codes are distinguishable: `quota_exceeded` vs `feature_locked` vs `authorization_error`

## Error code conventions
- `quota_exceeded` (403): user hit a usage limit (monthly backtests, templates)
- `feature_locked` (403): feature requires a higher plan tier
- `authorization_error` (403): generic access denial (ownership, etc.)
- `not_found` (404): resource does not exist or is not owned by the user
- `validation_error` (422): input failed schema or business rule validation
- `data_unavailable` (422): market data provider could not serve the request
- `rate_limited` (429): per-user rate limit exceeded

## Deployment
- Keep beat as a singleton process
- Worker processes all queues: research, market_data, exports, maintenance
- Migrations run before code deploys
- Health endpoints are connected to monitoring before launch
