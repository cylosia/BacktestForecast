# Workflow Trace Reference

This document is the operator-facing companion to `docs/known-limitations.md`. It describes the **current** runtime flow so support/on-call work does not rely on stale assumptions.

## Auth flow

- Browser/API requests primarily authenticate with Clerk Bearer tokens.
- SSR/stateful browser flows can fall back to the `__session` cookie.
- Cookie-authenticated state-changing requests are intentionally gated by `Origin`/`Referer` and `X-Requested-With` checks; do not remove those checks unless the SSR auth model changes.
- `GET /v1/meta` is public, but it *opportunistically* authenticates if a Bearer token or `__session` cookie is present. If the DB is unavailable during that lookup, the route degrades by returning unauthenticated metadata instead of failing the whole endpoint.

## Create flow

- Form submit/request payload -> route validation -> service enqueue/create -> dispatch helper writes outbox row + task metadata in the same transaction -> commit -> optimistic inline Celery send -> `maintenance.poll_outbox` fallback.
- A delayed queued job is no longer evidence of a “commit-first gap”; check the outbox state and stale-job repair path before assuming the create flow dropped work.
- Scan creation still computes some candidate metadata before enqueue, so very wide requests can add synchronous API latency before the job is handed off.

## Update flow

- Most updates are worker-driven status transitions or billing/webhook synchronization.
- Invariants often span status persistence, audit logging, and SSE publication; when debugging ordering, inspect the relevant component/service helper rather than assuming all side effects happen in one method.
- Billing cancellation audit writes are best-effort inside the cancellation path, but billing webhook events also have a fallback audit persistence path to avoid silent evidence loss.

## Delete / archive flow

- Delete endpoints are rate-limited and intentionally present a simpler surface than the underlying storage cleanup/cascade behavior.
- User-visible deletion success does not always imply all secondary cleanup happened synchronously; for export/storage investigations, confirm both DB state and storage backend state.

## Background job flow

- API create -> Celery task claim -> domain/service execution -> status persist + publish -> maintenance cleanup/reaper/outbox recovery.
- `poll_outbox`, stale-job repair, and reaper recovery are active runtime mechanisms, not scaffolding.
- Worker resource contention is still a practical concern because scans, sweeps, exports, and research workloads can compete for shared worker capacity.

## Export flow

- Export requests create a job record, workers render CSV/PDF, and the API serves the result from S3 or PostgreSQL blob storage.
- S3 is the preferred production path. PostgreSQL `content_bytes` remains supported, but DB-backed downloads still materialize full content in Python memory before streaming and can pressure API containers under concurrency.

## Billing / entitlement flow

- Pricing/checkout UI -> Stripe checkout -> webhook reconciliation -> user subscription state sync -> entitlement checks on create endpoints.
- Support incidents should validate both the Stripe event trail and the app-side entitlement state; hardcoded pricing/UI assumptions are a separate drift risk from webhook correctness.
- When billing revokes access, cancellation status publication happens only after the DB transaction commits so clients do not observe rolled-back cancels.

## Error / retry flow

- Primary resilience mechanisms are transactional outbox rows, `maintenance.poll_outbox`, stale-job repair, CAS-style updates, and task retries.
- If recovery appears confusing, prefer the runbook and task-specific helpers over old audit notes; the implementation has moved toward layered recovery and the documentation should be treated as operationally binding.
