# Documentation Index

This index separates **current operational guidance** from **historical audit archives** so operators do not treat old audit snapshots as live runtime truth.

## Current operational docs

| Document | Purpose | Owner | When to update |
|---|---|---|---|
| `docs/workflow-trace.md` | Authoritative current operational assumptions for auth, dispatch, queues, billing, exports, and retries | API + worker maintainers | Any change to runtime behavior or recovery semantics |
| `docs/RUNBOOK.md` | Incident response and diagnosis steps | On-call rotation + API/worker maintainers | Any incident response change or new operator procedure |
| `docs/known-limitations.md` | Still-open constraints and tradeoffs | Service/domain owner for the affected subsystem | When a limitation is added, removed, or materially changes |
| `docs/monitoring-alerting.md` | Metrics, alerts, and dashboards | Observability / platform maintainers | When telemetry or alert contracts change |
| `docs/backtest-strategy-assumptions.md` | Strategy-modeling assumptions and caveats | Backtest engine maintainers | When strategy behavior or user-facing assumptions change |

## Historical audit archives

These files are retained for traceability and planning context, but they are **not** primary navigation for current runtime behavior:

- `docs/audit/`
- `docs/audit-log.md`
- `docs/audit-open-items-2026-03-20.md`
- `docs/audit-remaining-items.md`
- `docs/audit-remaining-items2.md`

If a historical audit conflicts with a current operational doc, treat the current operational doc plus the referenced code path as authoritative.
