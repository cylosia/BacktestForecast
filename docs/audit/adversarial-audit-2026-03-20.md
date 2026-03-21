## 1. Executive Summary
- Overall codebase quality: above-average discipline for a fast-moving monorepo, but not production-safe at the standard you requested. The code shows strong intent around contracts, observability, and tests, yet still contains architecture-level correctness hazards, configuration/import-time side effects, frontend/backend drift, and hidden workflow split-brain points.
- Deployment risk level: High. The most dangerous defects are not syntax bugs; they are workflow integrity failures, stale comments masking live paths, contract drift hidden behind `any`, and operational assumptions that will only fail under process crashes, configuration churn, or partial outages.
- Top 20 critical issues:
  1. Create endpoints claim transactional outbox semantics, but job creation is committed before dispatch/outbox persistence, leaving crash windows where queued jobs can be stranded forever.
  2. Same split-transaction flaw exists across backtests, scans, sweeps, analyses, and exports.
  3. Idempotency turns stranded queued jobs into durable user-facing deadlocks because retries return the original stuck record.
  4. Daily picks history UI only fetches 10 items and ignores backend cursor pagination, silently truncating history.
  5. Templates page uses `any` to paper over response shape uncertainty, defeating the OpenAPI-generated contract.
  6. `usePolling` marks a resource `done` before `onComplete` succeeds, then can revert to `error`, creating false-negative terminal UX.
  7. `Settings` emits a runtime warning at import when `MASSIVE_API_KEY` is absent; with warnings-as-errors this breaks app/test startup.
  8. API settings are captured at import time for middleware-critical behavior, so runtime invalidation is partial and misleading.
  9. SSE backend comment explicitly says the frontend does not consume SSE, but the frontend does; this is stale operational documentation in code for a live path.
  10. App layout and pages repeatedly refetch `/v1/me`, multiplying latency and auth/API load on every render.
  11. Backtest read endpoints use write-capable DB sessions instead of read-only sessions.
  12. Same pattern exists on several status/detail endpoints, needlessly increasing write-pool pressure.
  13. Daily picks UX hardcodes “4:00 AM UTC” while worker beat config schedules the pipeline at 6:00 UTC.
  14. Hard-coded API/version constants are duplicated in multiple surfaces rather than derived from package version.
  15. Production safety depends on comments and discipline more than hard invariants in several critical workflows.
  16. Configuration reload support is partial: some code paths re-read settings, some remain frozen for process lifetime.
  17. Read replica support exists, but many “read” paths still hit primary sessions.
  18. Frontend server components make repeated token fetches and repeated authenticated backend round trips within the same request.
  19. Audit/event/logging discipline is strong but inconsistently coupled to real transaction boundaries.
  20. Test suite signal is weakened by environment-sensitive startup side effects.
- Top 20 likely production incidents:
  1. User submits job, sees perpetual `queued`, retries, and keeps getting the same dead job.
  2. Broker/process crash between create commit and dispatch commit creates invisible stuck jobs.
  3. Daily picks history appears to “work” while omitting most historical runs.
  4. UI status flips from success to error because a navigation/refresh callback failed after terminal completion.
  5. Production config hot change appears applied but middleware continues using stale hosts/CORS/body limits.
  6. Read traffic competes with write traffic on the primary DB under load.
  7. Layout-level `/me` fetch amplifies latency and backend load across all app pages.
  8. Tests or local startup fail because warnings are promoted to errors.
  9. On-call engineer trusts SSE comment and debugs the wrong path.
  10. History/detail pages regress silently because `any` masks contract breaks.
  11. Export/backtest/scan/sweep create flows wedge after transient dispatch outage.
  12. Customer support sees “missing” history because UI pagination is client-side over a truncated server slice.
  13. Metrics/health/readiness disagree with real runtime configuration after invalidation.
  14. Frontend message about pipeline schedule misleads users and support.
  15. A partial deploy changes package version but `/meta` and `/health` still report stale constants.
  16. Background queues recover, but previously stranded jobs never do because they never got outbox rows.
  17. Retry storms increase because stuck idempotent jobs are not self-healing.
  18. Read replica remains underused despite availability, hiding scaling headroom.
  19. Hidden session mutability on “read” endpoints causes accidental rollback warnings/noise.
  20. Runtime-config reload assumptions lead to inconsistent security posture across worker/API processes.
- Confidence: medium-high. I fully verified repository structure, main request/dispatch/worker flows, key services, schemas, repositories, frontend data layer, SSE path, and migration/config behavior. I inferred some production blast-radius and operational failure modes from confirmed code paths rather than live deployment telemetry.

## 2. System Map
- Architecture summary:
  - `apps/api`: FastAPI application exposing authenticated `/v1/*` endpoints plus health and SSE.
  - `apps/worker`: Celery worker/beat process running research, export, maintenance, and nightly pipeline tasks.
  - `apps/web`: Next.js app using Clerk for auth, server components for page data loading, and client components for form submission/polling/SSE.
  - `src/backtestforecast`: shared domain code: schemas, services, repositories, models, billing, market data, backtest engine, pipeline, security, observability.
  - `packages/api-client`: OpenAPI-derived TS client types consumed by the web app.
- Key services/modules:
  - Auth: `apps/api/app/dependencies.py`, `src/backtestforecast/auth/verification.py`.
  - Backtests: router + `BacktestService` + `BacktestExecutionService` + engine.
  - Scans/Sweeps/Analyses: async create/read/delete workflows with Celery dispatch.
  - Exports: async export job lifecycle with DB/S3 storage.
  - Billing: Stripe checkout/portal/webhooks/reconciliation/cancellation.
  - Pipeline/Daily picks: nightly scheduled pipeline writes recommendations consumed by API/web.
  - Events/SSE: Redis Pub/Sub based status streaming, proxied through Next route.
- Main workflows:
  - Clerk token/cookie → FastAPI auth dependency → user lookup/create.
  - Create job endpoint → service persists queued row → dispatch helper writes task id + outbox → Celery worker executes → repository/service writes terminal state → UI polls/SSE refreshes.
  - Stripe webhook → claim event row → sync user entitlement state → optionally cancel in-flight jobs → publish cancellation events post-commit.
  - Nightly pipeline beat task → market data fetch → ranking/recommendations → daily picks API/web rendering.
- Trust boundaries:
  - Browser ↔ Next web app.
  - Next web app ↔ FastAPI API.
  - FastAPI/worker ↔ Postgres.
  - FastAPI/worker ↔ Redis (rate limiting, cache, SSE, broker/backend).
  - FastAPI/worker ↔ Massive API.
  - FastAPI ↔ Stripe webhooks/API.
  - Worker/API ↔ optional S3 storage.
- Critical dependencies:
  - Clerk JWT/JWKS availability.
  - Postgres transactional integrity.
  - Redis availability and separation assumptions.
  - Celery broker/backend health.
  - Massive market data quality/latency.
  - Stripe event ordering/idempotency.

## 3. Critical Findings
1. Severity: Critical
   - Category: correctness / ops
   - Location: `apps/api/app/dispatch.py`, create routers, and create services.
   - Evidence: `dispatch_celery_task()` documents that the outbox row is written “in the same transaction as the job record,” but create services commit the job before routers call `dispatch_celery_task()`. The dispatch helper only starts working after that earlier commit.
   - Why it is a problem: the claimed transactional outbox guarantee is false at the real entry points.
   - Real-world failure mode: API process crashes after job row commit but before dispatch/outbox commit; user owns a permanent queued job with no outbox recovery path.
   - How to fix it: move job creation + outbox write + task-id persistence into one service-layer transaction per job type. Routers must not commit/create before dispatch state is persisted.
   - Fix priority: P0.
2. Severity: Critical
   - Category: correctness
   - Location: backtests create flow.
   - Evidence: `BacktestService.enqueue()` commits at the end; router then calls `dispatch_celery_task()`. Same split-brain pattern as above.
   - Why it is a problem: backtest jobs can be orphaned in `queued`.
   - Real-world failure mode: user sees a backtest that never starts and cannot recover via same idempotency key.
   - How to fix it: collapse enqueue+dispatch into one transactional service method.
   - Fix priority: P0.
3. Severity: Critical
   - Category: correctness
   - Location: scans create flow.
   - Evidence: `ScanService.create_job()` commits before router dispatch.
   - Why it is a problem: orphaned scan jobs are possible.
   - Real-world failure mode: scans remain queued forever with no outbox row.
   - How to fix it: same as above.
   - Fix priority: P0.
4. Severity: Critical
   - Category: correctness
   - Location: sweeps create flow.
   - Evidence: `SweepService.create_job()` commits before router dispatch.
   - Why it is a problem: same queue/orphan bug on the heaviest workload.
   - Real-world failure mode: expensive sweep requests wedge and consume user trust/support time.
   - How to fix it: same as above.
   - Fix priority: P0.
5. Severity: Critical
   - Category: correctness
   - Location: analysis create flow.
   - Evidence: `SymbolDeepAnalysisService.create_analysis()` commits before router dispatch.
   - Why it is a problem: same orphan risk for deep analysis jobs.
   - Real-world failure mode: analysis appears queued forever; retry returns same stuck record.
   - How to fix it: same as above.
   - Fix priority: P0.
6. Severity: Critical
   - Category: correctness
   - Location: exports create flow.
   - Evidence: `ExportService.enqueue_export()` commits before router dispatch.
   - Why it is a problem: exports can be stranded before outbox persistence.
   - Real-world failure mode: user never receives export and repeated request reuses stuck job.
   - How to fix it: same as above.
   - Fix priority: P0.
7. Severity: High
   - Category: correctness / frontend-backend mismatch
   - Location: daily picks history web path.
   - Evidence: frontend fetcher hard-caps history to `limit=10`, backend supports cursor pagination, and client component paginates only within whatever 10 items were fetched.
   - Why it is a problem: historical data is silently truncated while UI implies paging support.
   - Real-world failure mode: operators and users believe only a handful of pipeline runs exist.
   - How to fix it: plumb backend cursor through server/page props and render true server-side pagination.
   - Fix priority: P1.
8. Severity: High
   - Category: maintainability / frontend-backend mismatch
   - Location: templates page.
   - Evidence: page uses `(template as any).config_json ?? template.config`.
   - Why it is a problem: the typed contract is being bypassed on a rendered page, which hides schema drift and lets breakage ship silently.
   - Real-world failure mode: backend response shape changes, compile still passes, page misrenders or crashes at runtime.
   - How to fix it: use the generated typed field only; fix the real serializer/client type if it is wrong.
   - Fix priority: P1.
9. Severity: High
   - Category: correctness / UX
   - Location: `apps/web/hooks/use-polling.ts`.
   - Evidence: hook sets `status="done"` before `onComplete` resolves, then downgrades to `error` if callback fails.
   - Why it is a problem: terminal resource success is conflated with follow-up UI callback success.
   - Real-world failure mode: completed job shown as failed because page refresh/navigation threw.
   - How to fix it: separate resource terminal status from post-completion callback status.
   - Fix priority: P1.
10. Severity: High
   - Category: testing / ops
   - Location: `src/backtestforecast/config.py` import-time validator.
   - Evidence: non-production settings emit `warnings.warn()` when `MASSIVE_API_KEY` is absent.
   - Why it is a problem: repo pytest config promotes warnings to errors, so ordinary startup/tests can fail on environment shape instead of business logic.
   - Real-world failure mode: CI/local checks fail or become environment-coupled; engineers ignore warning noise.
   - How to fix it: log once instead of warning, or gate by explicit dev-only flag.
   - Fix priority: P1.
11. Severity: High
   - Category: ops / maintainability
   - Location: `apps/api/app/main.py`.
   - Evidence: `_startup_settings = get_settings()` and comments acknowledge middleware args are frozen for process lifetime.
   - Why it is a problem: repo advertises runtime invalidation support, but the most security-sensitive middleware config stays stale.
   - Real-world failure mode: rotated hosts/CORS/body-limit settings appear changed but are not enforced until restart.
   - How to fix it: either remove invalidation claims and require restart, or make middleware settings dynamic.
   - Fix priority: P1.
12. Severity: Medium
   - Category: maintainability / correctness
   - Location: `apps/api/app/routers/events.py` vs `apps/web/hooks/use-sse.ts`.
   - Evidence: SSE backend docstring says frontend uses polling exclusively and has no active SSE consumers, but web app actively instantiates `EventSource` through `useSSE`.
   - Why it is a problem: stale code commentary is now operationally wrong.
   - Real-world failure mode: incident response/debugging follows dead assumptions.
   - How to fix it: update docs/comments and add end-to-end SSE coverage.
   - Fix priority: P2.
13. Severity: Medium
   - Category: performance
   - Location: `apps/web/app/app/layout.tsx` and page-level data loaders.
   - Evidence: layout fetches `/v1/me`, pages fetch `/v1/me` again.
   - Why it is a problem: redundant authenticated backend round trips on nearly every page render.
   - Real-world failure mode: unnecessary latency and higher auth/API load under traffic.
   - How to fix it: memoize/shared request-scoped user fetch in server components.
   - Fix priority: P2.
14. Severity: Medium
   - Category: performance / architecture
   - Location: multiple GET detail/status endpoints.
   - Evidence: several read endpoints depend on `get_db` instead of `get_readonly_db`.
   - Why it is a problem: avoidable primary DB and write-pool pressure.
   - Real-world failure mode: reduced headroom during read-heavy usage.
   - How to fix it: switch pure reads to read-only sessions consistently.
   - Fix priority: P2.
15. Severity: Medium
   - Category: correctness / UX
   - Location: daily picks page copy vs worker schedule.
   - Evidence: web text says nightly run happens at 4:00 AM UTC; worker beat schedule is 6:00 UTC.
   - Why it is a problem: user-facing schedule is wrong.
   - Real-world failure mode: support confusion and false outage reports.
   - How to fix it: derive schedule text from a single source or update copy.
   - Fix priority: P2.
16. Severity: Medium
   - Category: maintainability
   - Location: version reporting surfaces.
   - Evidence: `/meta` and health use hard-coded `0.1.0`, while app version also exists centrally.
   - Why it is a problem: version drift across surfaces is likely.
   - Real-world failure mode: operators inspect mismatched versions during rollback/debugging.
   - How to fix it: derive from package version in one place.
   - Fix priority: P2.
17. Severity: Medium
   - Category: security / ops
   - Location: SSE proxy origin handling.
   - Evidence: route allows requests with no Origin/Referer and relies on auth token as primary gate.
   - Why it is a problem: acceptable for non-browser clients, but this mixes browser and server threat models in a cookie-backed auth path.
   - Real-world failure mode: future auth/cookie changes could accidentally widen cross-site exposure.
   - How to fix it: explicitly distinguish browser vs non-browser access or require same-origin for browser contexts.
   - Fix priority: P2.
18. Severity: Medium
   - Category: correctness
   - Location: create-job idempotency paths across services.
   - Evidence: nonterminal existing job is returned immediately for duplicate idempotency keys.
   - Why it is a problem: once a job is orphaned in queued state, idempotency cements the failure.
   - Real-world failure mode: user cannot self-heal with retries.
   - How to fix it: detect stale queued jobs and either re-dispatch or permit safe replacement.
   - Fix priority: P1.
19. Severity: Medium
   - Category: maintainability
   - Location: templates serialization path.
   - Evidence: server model coercion + frontend `any` access implies the contract is not trusted end-to-end.
   - Why it is a problem: this is a systemic smell, not just one page bug.
   - Real-world failure mode: future template schema evolution breaks one side silently.
   - How to fix it: add explicit contract tests for template response shape and remove escape hatches.
   - Fix priority: P2.
20. Severity: Medium
   - Category: testing
   - Location: environment-sensitive repo checks.
   - Evidence: migration drift/openapi scripts require environment that is not isolated in the script contract.
   - Why it is a problem: false negatives reduce trust in automation.
   - Real-world failure mode: engineers stop running or trusting the checks.
   - How to fix it: make scripts self-configuring or fail with actionable precondition messages.
   - Fix priority: P3.

## 4. Frontend-Backend Contract Mismatches
- Daily picks history backend supports cursor pagination, but `getDailyPicksHistory()` does not expose cursor and the page always fetches only the first 10 items.
- Templates page bypasses the contract by reaching for `config_json` even though `TemplateResponse` already exposes `config`.
- Polling hook reports UI callback failure as resource failure, so frontend state no longer represents backend truth.
- User/session fetch is duplicated across layout and pages rather than sharing a single server-side contract.
- User-facing daily-picks schedule copy disagrees with backend beat schedule.

## 5. Database / Migration / Schema Drift Findings
- No catastrophic current model↔migration mismatch was found in the inspected high-risk tables after following the full migration chain.
- The larger problem is transactional drift: create services persist rows in one transaction while dispatch/outbox state is persisted in another, so the effective workflow state machine does not match the intended schema design.
- Readonly/readwrite session usage drifts from the architectural intent; several read endpoints still consume primary write-capable sessions.
- Template rendering contract drift is visible in the frontend’s `any` fallback.

## 6. Workflow Trace Findings
- Auth flow:
  - Clerk bearer/cookie auth is reasonably defensive.
  - Weakness: multiple pages redundantly resolve the same auth-backed `/me` data, increasing surface/latency.
- Create flow:
  - Broken at architecture level: record commit happens before dispatch/outbox commit for every major async job type.
- Update flow:
  - Template optimistic concurrency exists, but the frontend still does not fully trust or enforce the typed template contract.
- Delete/archive flow:
  - Destructive delete paths generally exist, but read endpoints still sometimes use write sessions, so “safe read” boundaries are blurry.
- Background job flow:
  - Worker code is comparatively mature, but the producer-side split transaction undermines the outbox promise before tasks ever reach the worker.
- Export/import flow:
  - Export download path has strong content-safety checks, but export creation still inherits the split dispatch bug.
- Billing/entitlement flow:
  - Billing logic is careful about Stripe ordering, but user-visible job cancellation still depends on correct post-commit event publication and strong operational understanding.
- Error/retry flow:
  - Idempotency + orphaned queued jobs create durable retry traps instead of recovery.

## 7. Performance Findings
1. Duplicate `/v1/me` fetches from layout + pages.
2. Pure reads still hitting primary/write sessions.
3. Frontend pages perform multiple authenticated server->API calls that could be request-scoped and deduplicated.
4. SSE path is live but internal comments/documentation are stale, which increases tuning/operational cost.
5. Daily picks history client pagination is useless because data is truncated at fetch time, wasting UI complexity without delivering actual navigation.

## 8. Security Findings
1. Runtime config invalidation is partial; middleware-critical security config remains frozen until restart.
2. SSE proxy origin logic is permissive for no-Origin/no-Referer requests; safe today, brittle under future auth changes.
3. Contract escape hatches (`any`, stale comments, duplicate config/version constants) are not direct exploits, but they are security-adjacent because they undermine operator confidence in what is actually enforced.

## 9. Testing Gaps
- Missing end-to-end test that simulates crash between create commit and dispatch commit.
- Missing tests asserting idempotent retries can recover stale queued jobs.
- Missing contract tests proving daily-picks history pagination is wired through the web layer.
- Missing tests forbidding `any`-based template contract bypasses.
- Missing tests around runtime config invalidation and middleware behavior after settings change.
- Missing load tests validating primary vs read-replica utilization on read endpoints.

## 10. Dead Code / Confusing Code / Refactor Targets
- SSE backend docstring claiming no frontend consumers is stale and misleading.
- Template page `any` fallback should be removed.
- Hard-coded version constants should be centralized.
- Split create+dispatch responsibilities across router/service layers are misleading abstractions and should be collapsed.
- Runtime invalidation callbacks create the impression of hot-reloadable config, but middleware freezing contradicts that model.

## 11. Quick Wins
- Move each create flow to a single transaction that includes job row + task id + outbox row.
- Add stale-queued-job recovery for idempotency collisions.
- Wire cursor pagination through daily-picks history web flow.
- Remove `any` from templates page and fail hard on contract drift.
- Change the dev `MASSIVE_API_KEY` warning to structured logging.
- Switch read-only endpoints to `get_readonly_db` where no mutation occurs.
- Deduplicate `/v1/me` on the web server request path.
- Update daily-picks schedule copy.
- Replace hard-coded version strings with a shared constant.
- Update SSE comments and add a thin e2e test proving the path is live.

## 12. Highest-Value Refactor Plan
- Immediate hotfixes:
  - Collapse create+dispatch into single transactional methods for all async job producers.
  - Add stale queued job remediation on idempotency reuse.
  - Fix daily-picks history pagination path.
  - Remove template `any` fallback.
- Short-term stabilization:
  - Replace import-time warnings with logs.
  - Normalize read-only endpoint session usage.
  - Centralize version derivation.
  - Deduplicate `/me` data loading in Next server components.
- Medium-term refactors:
  - Move queue-producing logic entirely into services; routers should orchestrate HTTP only.
  - Make config reload semantics explicit: either dynamic everywhere or restart-required everywhere.
  - Add contract tests for every web page that consumes paginated API data.
- Long-term architectural improvements:
  - Introduce a true command/outbox abstraction instead of ad hoc create-then-dispatch flows.
  - Add saga/recovery tooling for stranded jobs.
  - Unify frontend data access through request-scoped cache/fetch helpers.

## 13. Appendix: File-by-File Notes
- `apps/api/app/main.py`: import-time settings freeze middleware-critical config; operationally dangerous.
- `apps/api/app/dispatch.py`: helper is internally coherent, but its guarantee is invalidated by caller transaction boundaries.
- `apps/api/app/routers/backtests.py`: create flow split across two commits; read endpoints use write sessions.
- `apps/api/app/routers/scans.py`: same split create/dispatch defect.
- `apps/api/app/routers/sweeps.py`: same split create/dispatch defect.
- `apps/api/app/routers/analysis.py`: same split create/dispatch defect.
- `apps/api/app/routers/exports.py`: same split create/dispatch defect.
- `apps/api/app/routers/daily_picks.py`: backend cursor support exists; web layer does not honor it.
- `apps/api/app/routers/events.py`: SSE implementation is real; commentary is stale.
- `src/backtestforecast/config.py`: strong production validation, but dev warning side effects are test-hostile.
- `src/backtestforecast/services/backtests.py`: enqueue commit before dispatch is the core defect.
- `src/backtestforecast/services/scans.py`: same.
- `src/backtestforecast/services/sweeps.py`: same.
- `src/backtestforecast/services/exports.py`: same.
- `src/backtestforecast/pipeline/deep_analysis.py`: same pattern in analysis creation.
- `apps/web/lib/api/server.ts`: daily-picks history cursor omitted; repeated token/user fetches.
- `apps/web/app/app/layout.tsx`: redundant `/me` fetch at layout scope.
- `apps/web/app/app/daily-picks/page.tsx`: fetches only first history slice and shows wrong pipeline schedule text.
- `apps/web/components/daily-picks/picks-history.tsx`: paginates only the truncated client-side slice.
- `apps/web/app/app/templates/page.tsx`: `any` erases schema guarantees.
- `apps/web/hooks/use-polling.ts`: callback failure contaminates terminal job status.

## Top 100 fixes in exact implementation order
1. Refactor backtest create path into one transactional service method.
2. Do the same for scans.
3. Do the same for sweeps.
4. Do the same for analyses.
5. Do the same for exports.
6. Add regression tests for crash-between-create-and-dispatch for backtests.
7. Add equivalent tests for scans.
8. Add equivalent tests for sweeps.
9. Add equivalent tests for analyses.
10. Add equivalent tests for exports.
11. Add stale queued job detection on idempotency reuse.
12. Add explicit re-dispatch flow for stale queued jobs.
13. Record orphan-detection metrics.
14. Add admin/runbook procedure for stranded jobs.
15. Convert daily-picks web history to backend cursor pagination.
16. Surface `next_cursor` in the page URL.
17. Add history pagination e2e coverage.
18. Remove template page `any` fallback.
19. Add template response contract test in web layer.
20. Fail CI on `any` in app pages touching API payloads.
21. Split resource terminal status from UI callback status in `usePolling`.
22. Add regression tests for callback failure after terminal success.
23. Replace `warnings.warn` in config with structured logging.
24. Add explicit env precondition docs for data-fetching features.
25. Make test bootstrap resilient to missing data-provider credentials.
26. Switch backtest status/detail reads to `get_readonly_db` where safe.
27. Audit scanner reads for readonly usage.
28. Audit sweep reads for readonly usage.
29. Audit analysis reads for readonly usage.
30. Audit export reads for readonly usage.
31. Add request-scoped memoization for `getCurrentUser()` in web server components.
32. Stop duplicate `/v1/me` fetches in layout + page.
33. Add performance budget/assertion for page data round trips.
34. Replace hard-coded daily-picks schedule text with config-derived text.
35. Centralize version constants for `/meta`, `/health`, and package version.
36. Remove stale SSE comment.
37. Add SSE e2e coverage from Next proxy to API backend.
38. Clarify config reload semantics in docs and code.
39. Either remove invalidation callbacks for frozen middleware or make middleware dynamic.
40. Add startup log that enumerates config surfaces requiring restart.
41. Add dashboard/alert for queued jobs older than dispatch SLA.
42. Add reconciliation job for jobs with `queued` + null `celery_task_id` + no outbox.
43. Add API-visible “stuck” status or diagnostic error code.
44. Add support tooling to requeue or fail stranded jobs safely.
45. Assert outbox row exists for every newly queued job in tests.
46. Audit all create services for hidden pre-dispatch commits.
47. Move router-side dispatch calls into services.
48. Keep routers side-effect free except HTTP serialization.
49. Add tracing span around enqueue+dispatch transaction.
50. Log correlation between job row id and outbox id.
51. Add metric for idempotent duplicate returns by status.
52. Alert on repeated duplicates to stale queued jobs.
53. Add frontend UI to explain stale/repairing queued states.
54. Add pagination support to daily-picks history UI controls.
55. Add deep links from analysis history rows to detail pages.
56. Add consistent typed serializers for template responses.
57. Remove frontend fallback reads of legacy field names.
58. Add schema snapshot test for template contract.
59. Use shared request cache for token retrieval in server fetch helpers.
60. Profile page render waterfalls and parallelize safely.
61. Move static schedule copy into one documented source.
62. Add runbook note for config changes requiring restart.
63. Add negative test for changing CORS/body-limit config at runtime.
64. Add read-replica usage metrics by endpoint.
65. Move more list/detail endpoints to read replica.
66. Add circuit-breaker/timeout observability for Massive-dependent pages.
67. Add explicit fallback UX for read replica unavailable conditions.
68. Ensure health/version values come from one source of truth.
69. Add lint rule or custom checker for hard-coded version strings.
70. Add contract test ensuring every paginated backend list has web cursor plumbing.
71. Add contract test ensuring no page uses `any` on API types.
72. Add richer queue diagnostics to admin/health endpoints.
73. Document stranded-job recovery semantics publicly/internal.
74. Add row-level “dispatch_started_at” or equivalent on all job tables if needed.
75. Add synthetic monitoring for create→running latency.
76. Alert if queued jobs exceed threshold without outbox records.
77. Make idempotency semantics explicit in API docs.
78. Add retry-safe replace semantics for abandoned queued jobs.
79. Add consistency check between task route names and SSE resource names.
80. Audit user-facing copy for schedule/timezone accuracy.
81. Add shared schedule formatter utility.
82. Stop console-error logging from server components where it creates noisy logs.
83. Normalize all read endpoints to `Cache-Control` semantics intentionally.
84. Audit use of write sessions in GET routes repo-wide.
85. Add integration test for runtime config invalidation if kept.
86. Add static analysis check for service-layer commits before router dispatch.
87. Consider explicit unit-of-work abstraction around DB + outbox.
88. Add dead-letter/recovery path for producer-side dispatch failures.
89. Add support action to regenerate failed exports after storage errors.
90. Add audit event coverage for repair/requeue flows.
91. Add migration/check scripts precondition handling and clearer errors.
92. Make local check scripts auto-load `.env` or documented defaults.
93. Add page-level loading budgets in web tests.
94. Add benchmark for `/app/*` layout data fetch duplication.
95. Add failure injection tests for broker outage during create.
96. Add failure injection tests for process crash after create commit.
97. Add failure injection tests for Redis outage during SSE and rate-limit paths.
98. Periodically review stale comments vs live behavior.
99. Introduce architecture decision record for async job lifecycle guarantees.
100. Re-audit after these fixes; many current lower-level findings may disappear once transactional integrity is corrected.

## Top 20 bugs most likely already affecting production
1. Orphaned queued jobs from create/dispatch split.
2. Retries returning the same stuck idempotent job.
3. Daily-picks history silently missing older runs.
4. False UI error after successful terminal completion callback failure.
5. Duplicate `/me` fetch latency on app pages.
6. Read traffic unnecessarily hitting primary DB.
7. Wrong daily-picks schedule text causing support confusion.
8. Stale config after hot change without restart.
9. Template contract regressions masked by `any`.
10. SSE maintenance/debug confusion from stale comment.
11. Misreported version strings across surfaces.
12. Unreliable local/CI startup when warnings are fatal.
13. Hidden support load from “queued forever” incidents.
14. Underused read replica increasing infra spend on primary.
15. Misleading history pagination UI that never reaches older data.
16. Operational drift between intended outbox design and real behavior.
17. Silent absence of self-healing for stranded jobs.
18. Noisy logs from repeated current-user fetch failures in layout.
19. Partial runtime config invalidation creating split-brain behavior.
20. Frontend confidence in generated API client undermined by manual escape hatches.

## Top 20 things that look correct but may be silently wrong
1. “Transactional outbox” guarantee in dispatch helper.
2. Idempotency safety for create endpoints.
3. Daily-picks history pagination UI.
4. Template page type safety.
5. Polling status truthfulness.
6. Runtime config invalidation support.
7. Read replica usage coverage.
8. SSE backend documentation.
9. Version reporting consistency.
10. Startup/test stability under warning policies.
11. User-facing nightly schedule copy.
12. Service/router responsibility boundaries.
13. Queue recovery assumptions after producer crash.
14. Per-page auth/API fetch efficiency.
15. “Read” endpoint database session choice.
16. OpenAPI/client contract confidence on templates.
17. Operational understanding of live SSE usage.
18. Support assumptions about missing history.
19. Hot-config security changes taking effect immediately.
20. The belief that passing unit tests would catch these workflow-integrity failures.
