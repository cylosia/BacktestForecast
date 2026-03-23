# Current operational assumptions

This is the authoritative operator-facing index for **what the system assumes today**.

Use this page first, then follow the linked detailed docs:

- `docs/workflow-trace.md` — request → enqueue → dispatch/outbox → worker → cleanup/recovery flow.
- `docs/RUNBOOK.md` — diagnosis and incident response procedures.
- `docs/known-limitations.md` — still-open technical/product limitations only.
- `docs/monitoring-alerting.md` — current metrics, alerts, and dashboard expectations.

## Runtime truths that operators should assume

- Async create flows persist queued job state and outbox metadata in the same transaction, then attempt inline broker delivery. `maintenance.poll_outbox` and stale-job repair are live recovery paths, not dormant scaffolding.
- Frontend and backend `target_dte` validation both allow values down to `1`; older notes about a frontend-only `>= 7` guard are stale.
- Billing webhook audit writes have a deferred fallback path. Failures emit `billing.audit_write_failed`, persist fallback payloads, and a maintenance drain task replays them.
- The pricing page is expected to render from the backend pricing contract (`/v1/billing/pricing`) rather than maintaining a separate frontend commercial contract.

## Historical audits

Audit documents under `docs/audit/` and older top-level audit snapshots are retained for traceability and planning context only. When an audit contradicts this page or the linked operational docs, prefer the current operational docs plus the referenced code path.
