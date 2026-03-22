# Refactor plan status

This note maps the audit's highest-value refactor plan to the code and tests
currently in the repository.

## Immediate hotfixes

- **Collapse create+dispatch into single transactional methods for all async job producers — addressed.**
  Service-level `create_and_dispatch*` flows are covered by targeted regression
  tests that assert the queued job and outbox state are persisted together
  across backtests, scans, sweeps, analyses, and exports.
- **Add stale queued job remediation on idempotency reuse — addressed.**
  Regression tests cover stale queued idempotency reuse for each async job
  producer and assert that the original job is re-dispatched instead of leaving
  the collision stranded.
- **Fix daily-picks history pagination path — addressed.**
  The web server helper uses the cursor pagination builder for
  `/v1/daily-picks/history`, and the page maps `next_cursor` in the URL back to
  the backend `cursor` query param.
- **Remove template `any` fallback — addressed.**
  The templates page and runtime contract validation path now rely on explicit
  validation rather than `any`-based page-layer bypasses.

## Short-term stabilization

- **Replace import-time warnings with logs — addressed.**
  Missing `MASSIVE_API_KEY` uses structured logging instead of `warnings.warn`.
- **Normalize read-only endpoint session usage — addressed.**
  Read-heavy routers use `get_readonly_db` / `get_current_user_readonly`, with
  guardrail tests to keep those dependencies in place.
- **Centralize version derivation — addressed.**
  Shared version/default constants live in `src/backtestforecast/version.py`
  and are consumed by runtime surfaces such as Prometheus metrics headers.
- **Deduplicate `/me` data loading in Next server components — addressed.**
  The web server layer memoizes token-keyed current-user loading, and request
  budget tests keep layout/page usage on the shared helper path.

## Medium-term refactors

- **Move queue-producing logic entirely into services; routers should orchestrate HTTP only — addressed.**
  Router guardrail tests assert queue-producing routers delegate to service
  `create_and_dispatch*` methods rather than importing the dispatch helper.
- **Make config reload semantics explicit: either dynamic everywhere or restart-required everywhere — addressed.**
  Startup logging and runtime-security tests document the reloadable vs
  restart-required split and verify runtime-resolved middleware behavior.
- **Add contract tests for every web page that consumes paginated API data — addressed.**
  Paginated page contract tests now cover backtests, scanner, sweeps, analysis,
  and daily-picks history wiring through the web layer.

## Long-term architectural improvements

- **Introduce a true command/outbox abstraction instead of ad hoc create-then-dispatch flows — not yet implemented.**
  The repo still uses per-service `create_and_dispatch*` helpers plus a shared
  dispatch helper, so this remains future architecture work.
- **Add saga/recovery tooling for stranded jobs — partially addressed.**
  Recovery exists today through stranded-job repair/reconciliation tooling, but
  it is not yet a full saga-style orchestration layer.
- **Unify frontend data access through request-scoped cache/fetch helpers — partially addressed.**
  The Next server data layer already uses cached fetch helpers, but the broader
  architectural unification remains an incremental improvement area rather than
  a finished migration.
