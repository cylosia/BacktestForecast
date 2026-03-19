# Data Retention Policy

## Account Deletion

When a user deletes their account via `DELETE /v1/account/me`:

### Immediately deleted (CASCADE)
- User record
- All backtest runs, trades, and equity points
- All scanner jobs and recommendations
- All sweep jobs and results
- All export jobs (including stored file content)
- All backtest templates
- All symbol analyses

### Retained for audit
- **Audit events** (`audit_events` table): User ID is set to NULL via `ON DELETE SET NULL`. The audit trail (event type, timestamps, IP hash) is preserved for compliance but is no longer linked to a specific user.
- **Stripe events** (`stripe_events` table): User ID is set to NULL via `ON DELETE SET NULL`. Webhook processing history is preserved for billing reconciliation.

### External systems
- **Stripe**: The subscription is cancelled and the customer object is deleted before the DB row is removed. If Stripe cleanup fails, the Stripe IDs are recorded in the audit event metadata for manual reconciliation.
- **Clerk**: The Clerk user account is NOT automatically deleted. The user must separately delete their Clerk identity if desired. A future enhancement could automate this via the Clerk Backend API.
- **Redis**: Cached data (option chains, rate limit counters) expires naturally via TTL. No explicit invalidation is performed.
- **S3**: Export files stored in S3 are orphaned when the export_jobs row is deleted. The `maintenance.reconcile_s3_orphans` task periodically cleans these up.

## Data Retention Periods

| Data | Retention | Mechanism |
|------|-----------|-----------|
| Audit events (high-volume) | 90 days | `maintenance.cleanup_audit_events` weekly task |
| Daily recommendations | 90 days | `maintenance.cleanup_daily_recommendations` weekly task |
| Export files | 30 days from creation | `expires_at` column, export cleanup task |
| Option data cache | 7 days | Redis TTL (`option_cache_ttl_seconds`) |
| DLQ messages | 30 days | Redis TTL + LTRIM to 5000 entries |
| Rate limit counters | 60 seconds | Redis TTL (window-based) |
| Outbox messages | Indefinite | No automatic cleanup; manual purge required |

## GDPR Considerations

- Account deletion is irreversible and removes all user-identifiable data from the database.
- IP addresses are stored only as SHA-256 hashes (one-way).
- Stripe customer/subscription IDs in audit events are retained for billing dispute resolution.
- A data export endpoint (`GET /v1/account/export`) is planned but not yet implemented. Users can export individual backtests via the existing CSV/PDF export feature before deleting their account.
