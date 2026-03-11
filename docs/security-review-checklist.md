# Security review checklist

## Application — implemented
- [x] `APP_ENV=production` validated: config raises `ValueError` if auth env vars missing in production.
- [x] `API_ALLOWED_HOSTS` validated: config raises `ValueError` if wildcard `*` in production.
- [x] Clerk JWT verification: bearer token required on all `/v1/*` endpoints (except webhook).
- [x] Request body limits enforced: `RequestBodyLimitMiddleware` with configurable `REQUEST_MAX_BODY_BYTES`.
- [x] Webhook path bypass: `/v1/billing/webhook` exempted from body-limit and Clerk auth.
- [x] CSV formula injection prevention: cells starting with `=`, `+`, `-`, `@` are single-quote prefixed.
- [x] Audit events retained: billing changes, export downloads, and scan operations are logged.
- [x] Rate limiting: per-user limits on backtest creation, scan creation, export creation, and billing operations.
- [x] Security headers: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy`, `Permissions-Policy`, `Cache-Control: no-store`.
- [x] CORS restricted: `WEB_CORS_ORIGINS` configured per-environment, credentials allowed.
- [x] Structured logging: JSON output enforced in production via `LOG_JSON=true` validation.
- [x] Stripe webhook signature verification: `construct_event` validates signature before processing.
- [x] Webhook dedupe: duplicate events detected via audit event existence check.
- [x] IP hashing: client IPs stored as SHA-256 hashes in audit events, not plaintext.

## Application — requires operator verification
- [ ] Stripe webhook secret is rotated through a secret manager.
- [ ] `LOG_JSON=true` is actually set in deployed environments.
- [ ] All env vars with secrets use a proper secret manager (not `.env` files in production).
- [ ] Clerk authorized parties match only the deployed web app origin.

## Infrastructure
- [ ] TLS terminates at the edge and internal traffic uses private networking where possible.
- [ ] PostgreSQL backups and restore drills are in place.
- [ ] Redis is deployed with auth and network restrictions.
- [ ] Worker, beat, and API use separate deploy units and health checks.
- [ ] Container images are pinned to digest or specific tags.

## Access / operations
- [ ] Admin access to hosting, Stripe, Clerk, and database is least-privilege.
- [ ] Deployment logs and audit logs are centralized.
- [ ] On-call runbook includes provider outage, DB outage, and webhook failure procedures.
