# Adversarial Production Audit — 2026-03-22

## 1. Executive Summary

- **Overall codebase quality:** above-average for a small product team, but still carrying multiple production-grade liabilities in correctness modeling, operational documentation drift, and throughput scaling. The codebase is test-heavy and has good defensive patterns in several places (rate limiting, auth hardening, CAS-style job state transitions, contract tests), but it is not yet in a state where I would call it “boring to operate.”
- **Deployment risk level:** **medium** for normal traffic, **high** for money-sensitive or trust-sensitive workflows (billing, long-running scans/sweeps, pricing communication, and options modeling claims).
- **Top 20 critical issues / highest-priority findings:**
  1. Scanner execution still accumulates up to 1000 candidate payloads in memory, including summaries/forecasts/trade lists/equity curves, which is an avoidable worker-memory bomb under broad universes or relaxed filters.
  2. Option-contract and quote fetches have no in-flight request coalescing, so concurrent scans/sweeps can stampede the external data provider and Redis cache.
  3. Billing audit writes are still best-effort only; if the DB is unhealthy during webhook processing, the billing state change can succeed while the audit trail silently disappears.
  4. The backtest engine now includes early-assignment heuristics, but they remain approximation-based rather than broker- or contract-specific, so assignment-sensitive strategies can still be directionally plausible and wrong at the edges.
  5. Wheel strategy accounting still uses float arithmetic internally and only reconciles at the end, leaving intermediate equity-curve points vulnerable to precision drift.
  6. The pricing page is hardcoded presentation data rather than a backend-driven contract, so customer-visible prices/features can drift from Stripe reality.
  7. `docs/known-limitations.md` is materially stale: it still documents a frontend/backend `target_dte` mismatch that no longer exists, claims the outbox is scaffolding only, and describes a commit-first dispatch gap that the current dispatch code no longer uses.
  8. DB-backed export downloads still materialize the whole blob in process memory; this is capped, but under concurrency it is still an avoidable memory-pressure path.
  9. Genetic sweeps still use `ThreadPoolExecutor` for CPU-bound fitness evaluation, limiting scale-up and wasting worker cores under heavy sweep traffic.
  10. `apps/worker/app/tasks.py` is still a 2277-line god file that couples pipeline, export, reaper, outbox, billing reconciliation, scan refresh, and core execution paths.
  11. `BillingService` is still nearly 1000 lines and mixes checkout, portal, webhook parsing, reconciliation, circuit breaking, and Stripe data translation.
  12. `ScanService` is still >1100 lines and mixes entitlement enforcement, dedupe/idempotency, execution, ranking, persistence, and serialization.
  13. Calendar spread now supports both call and put calendars through `strategy_overrides.calendar_contract_type`, but older clients/templates that never send the override will still default to call calendars.
  14. Naked option sizing is collateral-based and documented as understating theoretical risk; this is acceptable only if surfaced aggressively to users.
  15. Risk-free-rate handling is environment-static rather than date-aware, so Sharpe/Sortino values are operationally reproducible but financially stale when rates move.
  16. CI intentionally excludes load tests and the CD path still does not perform canary/weighted rollout or post-deploy workflow validation beyond health checks.
  17. Worker tests and many concurrency invariants are much better than average, but the architecture still depends on Redis/Postgres semantics that are hard to prove with the existing CI mix.
  18. Documentation drift is now large enough to become an operational defect, not just a docs problem.
  19. Several financially sensitive approximations are correctly documented, but still create “looks-right but is wrong” outputs for specific strategies and market regimes.
  20. The codebase has improved resilience mechanisms (outbox, stale-job reaper, CAS updates), but these are layered onto services that are still too large and too coupled.
- **Top 20 likely production incidents:**
  1. Scan worker OOM or eviction on large candidate sets.
  2. Redis/provider stampede from concurrent option-chain fetches.
  3. Stripe dispute/reconciliation pain because billing state changed but audit event did not persist.
  4. Users disputing covered-call results near ex-dividend dates due to missing early-assignment modeling.
  5. Pricing page and checkout behavior diverging after a Stripe price change.
  6. Stale runbooks causing operators to troubleshoot the wrong dispatch failure mode.
  7. Export downloads causing memory spikes when multiple DB-backed exports are downloaded simultaneously.
  8. Long sweep jobs monopolizing CPU while underutilizing available multiprocessing potential.
  9. Maintainers breaking task routing or beat schedules while editing the worker god file.
  10. Regression hidden inside `BillingService` because a local change accidentally impacts webhook or reconciliation behavior.
  11. Regression hidden inside `ScanService` because ranking, execution, and persistence are not isolated.
  12. Users receiving plausible but inaccurate risk metrics because `RISK_FREE_RATE` was not updated after a rate regime shift.
  13. Older clients/templates expecting generic calendar behavior but silently defaulting to call calendars because they never send the new override.
  14. Ops assuming the outbox poller is disabled because docs say so, while production actually depends on it.
  15. Ops assuming a target-DTE frontend constraint still exists and therefore missing low-DTE user behavior in telemetry/support.
  16. Post-deploy production issue escaping because CD verifies health endpoints, not end-to-end product actions.
  17. Redis running without TLS if moved off-box and operators follow the compose file without compensating controls.
  18. Job throughput collapse when scans, sweeps, and pipeline work overlap on shared provider and worker resources.
  19. Support tickets on intermediate wheel-equity chart discrepancies versus final totals.
  20. New engineers misreading the codebase because system behavior is scattered across giant files plus stale documentation.
- **Confidence level:** moderate-high. Verified directly from current source for architecture, dispatch, SSE, scans, exports, billing, pricing, CI/CD, and major strategy/modeling notes. Financial-correctness findings around assignment heuristics, wheel drift, and static risk-free-rate handling are partly direct (code/doc confirmed) and partly inferred from domain behavior. I did **not** exhaustively execute every workflow end-to-end against running infrastructure.

## 2. System Map

- **Architecture summary**
  - `apps/web`: Next.js frontend using Clerk auth, API wrapper helpers, polling/SSE, and API-client-generated TS types.
  - `apps/api`: FastAPI application, route layer, auth dependencies, middleware, dispatch helpers, readiness/live metrics.
  - `apps/worker`: Celery worker + beat + maintenance tasks, including reaper/outbox/pipeline/export/scan/sweep execution.
  - `src/backtestforecast`: shared domain package containing models, schemas, services, repositories, strategy engine, market-data integration, billing, resilience, and observability.
  - `alembic`: schema evolution.
- **Key services/modules**
  - Auth: `apps/api/app/dependencies.py`, `src/backtestforecast/auth/verification.py`.
  - Job dispatch/recovery: `apps/api/app/dispatch.py`, `src/backtestforecast/services/dispatch_recovery.py`, `apps/worker/app/tasks.py`.
  - Backtests: `src/backtestforecast/services/backtests.py`, `src/backtestforecast/backtests/engine.py`, strategy modules.
  - Scans: `src/backtestforecast/services/scans.py`, ranking logic in `src/backtestforecast/scans/ranking.py`.
  - Sweeps: `src/backtestforecast/services/sweeps.py`, GA in `src/backtestforecast/sweeps/genetic.py`.
  - Billing: `src/backtestforecast/services/billing.py`, `src/backtestforecast/billing/events.py`, `src/backtestforecast/repositories/stripe_events.py`.
  - Exports: `src/backtestforecast/services/exports.py`, `src/backtestforecast/exports/storage.py`, `apps/api/app/routers/exports.py`.
  - Real-time status: `apps/api/app/routers/events.py`, `apps/web/hooks/use-sse.ts`, poller components.
- **Main workflows**
  - Authenticated create flows: frontend form → API route → service enqueue → dispatch helper writes outbox + Celery task metadata → worker claims queued job → status updates via polling/SSE.
  - Read flows: frontend server/client components call API wrappers → FastAPI read routes → readonly DB session → repositories → typed response schemas.
  - Billing: UI triggers checkout/portal → Stripe-hosted page → webhook → webhook dedupe claim in DB → user subscription state sync + audit trail.
  - Export: user requests export → async job → worker renders file → DB/S3 storage → download endpoint streams or returns blob-backed content.
  - Nightly pipeline: beat → pipeline task → symbol universe → market/regime/backtest/forecast pipeline → stored daily picks.
- **Trust boundaries**
  - Browser ↔ Next.js server/client code.
  - Web ↔ API over auth-bearing requests.
  - API ↔ Redis/Postgres.
  - Worker ↔ Redis/Postgres.
  - App ↔ Massive market-data API.
  - App ↔ Stripe.
  - Optional app ↔ S3-compatible object storage.
- **Critical dependencies**
  - PostgreSQL: system of record for users/jobs/results/audit.
  - Redis: Celery broker, caching, rate limiting, SSE slot management, scheduling state.
  - Massive: required for new backtests/scans/sweeps/analysis.
  - Stripe: billing state authority.
  - Clerk: token verification / identity source.

## 3. Critical Findings

### CF-1
- **Severity:** High
- **Category:** performance
- **Location:** `src/backtestforecast/services/scans.py`, `_execute_scan`
- **Evidence:** The scan loop accumulates `candidates` in memory and only trims low-ranked heavy fields periodically. It still permits up to `_MAX_CANDIDATES_IN_MEMORY = 1000` before aborting, while each candidate stores request snapshot, summary, warnings, up to 50 trades, downsampled equity curve, historical payload, forecast, and ranking metadata.
- **Why it is a problem:** Memory pressure scales with candidate breadth instead of with `max_recommendations`. This is the wrong asymptotic behavior for a ranking pipeline.
- **Real-world failure mode:** Large manual scans or permissive scheduled scans trigger worker OOM, Linux cgroup eviction, or poor co-tenancy with other queues.
- **How to fix it:** Replace full accumulation with a bounded top-K heap plus periodic flush to a staging table or batched persistence. Keep only ranking keys + minimal metadata in memory until final selection.
- **Fix priority:** P0 before scaling scan traffic.

### CF-2
- **Severity:** High
- **Category:** performance / resilience
- **Location:** `src/backtestforecast/market_data/service.py`, `MassiveOptionGateway.list_contracts`, `get_quote`
- **Evidence:** The bar-fetch path has explicit coalescing elsewhere, but option contract and quote lookups directly call Redis/client after a simple local-cache miss. There is no shared in-flight dedupe for concurrent identical requests.
- **Why it is a problem:** Concurrent jobs requesting the same contract chain or quote race through to Redis/provider independently.
- **Real-world failure mode:** Provider throttling, inflated latency, noisy retries, or paying for redundant external calls.
- **How to fix it:** Apply the same in-flight future/event pattern already used for bars to contract and quote fetches. Cache “miss” results safely with short TTL.
- **Fix priority:** P0 for throughput protection.

### CF-3
- **Severity:** High
- **Category:** security / ops / compliance
- **Location:** `src/backtestforecast/billing/events.py`, `log_billing_event`
- **Evidence:** When `AuditService.record_always(...)` fails, the code logs `billing.audit_write_failed` and continues. There is no fallback queue, file, or replay channel.
- **Why it is a problem:** Billing state changes are among the most legally and financially sensitive actions in the product. Silent audit loss destroys reconstructability.
- **Real-world failure mode:** Charge dispute, manual reconciliation, or support investigation with no durable state-change trail even though subscription state changed.
- **How to fix it:** Persist failed audit payloads to a durable fallback (Redis list, file spool, or dedicated dead-letter table) and drain them asynchronously.
- **Fix priority:** P0.

### CF-4
- **Severity:** High
- **Category:** correctness
- **Location:** `src/backtestforecast/backtests/engine.py`; strategy assumptions in `docs/backtest-strategy-assumptions.md`
- **Evidence:** The engine already contains early-assignment heuristics, but they are rule-based approximations keyed off ex-dividend dates, moneyness, DTE, intrinsic value, and remaining time value rather than broker/borrow/borrow-cost or contract-specific exercise behavior.
- **Why it is a problem:** Assignment-sensitive strategies are better modeled than before, but the logic is still heuristic and can diverge from real-world assignment/exercise behavior.
- **Real-world failure mode:** Customer sees a backtest exit that is directionally right but mistimed versus actual broker assignment behavior.
- **How to fix it:** Continue refining assignment heuristics and label affected exits as approximation-based in result metadata.
- **Fix priority:** P1 for trust-sensitive strategy outputs.

### CF-5
- **Severity:** Medium
- **Category:** correctness / performance
- **Location:** `src/backtestforecast/backtests/strategies/wheel.py`
- **Evidence:** Existing audit notes and reconciliation behavior show wheel bookkeeping still uses float accumulation internally and fixes final drift after the fact.
- **Why it is a problem:** Final reconciliation does not make the intermediate path correct.
- **Real-world failure mode:** Equity curve or path-dependent metrics disagree slightly with final P&L, confusing users and making debugging harder.
- **How to fix it:** Convert internal cash/equity/state fields to `Decimal` end-to-end.
- **Fix priority:** P1.

### CF-6
- **Severity:** High
- **Category:** frontend-backend mismatch / correctness
- **Location:** `apps/web/app/pricing/page.tsx`, `src/backtestforecast/schemas/billing.py`, `apps/api/app/routers/billing.py`
- **Evidence:** This finding is stale. The pricing page now loads its pricing contract from `/v1/billing/pricing`, and checkout remains authoritative.
- **Why it is a problem:** The user sees one price/features list and is charged according to another authority.
- **Real-world failure mode:** Customer-visible price mismatch after Stripe price changes or feature-packaging edits.
- **How to fix it:** Add a backend pricing contract endpoint sourced from env/Stripe metadata and render the pricing page from that contract.
- **Fix priority:** P0 customer-facing trust issue.

### CF-7
- **Severity:** High
- **Category:** ops / maintainability
- **Location:** `docs/known-limitations.md` vs `apps/api/app/dispatch.py`, `apps/worker/app/celery_app.py`, `apps/web/lib/backtests/validation.ts`
- **Evidence:** Docs still claim the frontend enforces `target_dte >= 7`, that outbox is scaffolding only, that `poll_outbox` is disabled, and that dispatch uses a commit-first gap without outbox recovery. Current code contradicts all of those.
- **Why it is a problem:** Runbooks and “known limitations” are now lying about critical job-dispatch behavior.
- **Real-world failure mode:** On-call engineer debugs the wrong failure mode, support gives wrong answers, or product/business decisions use stale assumptions.
- **How to fix it:** Update or delete stale sections immediately and add a docs review step to CI for known operational invariants.
- **Fix priority:** P0 because this is now an operational correctness defect.

### CF-8
- **Severity:** Medium
- **Category:** performance
- **Location:** `apps/api/app/routers/exports.py`, `src/backtestforecast/services/exports.py`, `src/backtestforecast/exports/storage.py`
- **Evidence:** DB-backed exports use `content_bytes` already materialized in ORM memory. Router comments acknowledge large-download memory pressure and recommend server-side streaming / S3 migration.
- **Why it is a problem:** Memory cost is per concurrent download, not per file created.
- **Real-world failure mode:** API memory spikes or degraded latency during concurrent export downloads.
- **How to fix it:** Prefer S3 for large exports and introduce true streaming for DB storage or migrate all download paths to object storage.
- **Fix priority:** P1.

### CF-9
- **Severity:** Medium
- **Category:** performance
- **Location:** `src/backtestforecast/sweeps/genetic.py`, `_evaluate_population`
- **Evidence:** The code comment explicitly states `ThreadPoolExecutor` is suboptimal for CPU-bound fitness evaluation and keeps it only because the closure-based fitness function is hard to pickle.
- **Why it is a problem:** Worker CPU scaling is artificially capped by the GIL.
- **Real-world failure mode:** Long sweep latency and queue contention even on machines with spare cores.
- **How to fix it:** Refactor fitness evaluation into a serializable top-level callable and move to `ProcessPoolExecutor` or distributed parallelism.
- **Fix priority:** P1.

### CF-10
- **Severity:** Medium
- **Category:** maintainability / ops
- **Location:** `apps/worker/app/tasks.py`
- **Evidence:** 2277-line file mixing unrelated domains and failure handlers.
- **Why it is a problem:** High blast radius for changes, poor code ownership boundaries, brittle imports, and difficult reasoning about beat/task names.
- **Real-world failure mode:** Small worker change breaks unrelated queue or maintenance behavior.
- **How to fix it:** Split by domain and keep only Celery app registration / imports centralized.
- **Fix priority:** P1.

### CF-11
- **Severity:** Medium
- **Category:** maintainability
- **Location:** `src/backtestforecast/services/billing.py`
- **Evidence:** Single service handles checkout, portal, webhook verification, Stripe event interpretation, cancellation, reconciliation, and circuit-breaker behavior.
- **Why it is a problem:** Hard to unit-isolate and easy to regress across code paths.
- **Real-world failure mode:** Fixing one Stripe edge case breaks portal creation or reconciliation.
- **How to fix it:** Split into `WebhookHandler`, `CheckoutService`, `PortalService`, `ReconciliationService`, and a thin Stripe client abstraction.
- **Fix priority:** P1.

### CF-12
- **Severity:** Medium
- **Category:** maintainability / performance
- **Location:** `src/backtestforecast/services/scans.py`
- **Evidence:** Single file mixes entitlement, dedupe, execution orchestration, ranking, serialization, persistence, and list/detail response mapping.
- **Why it is a problem:** Prevents targeted optimization and makes scan correctness changes dangerous.
- **Real-world failure mode:** Ranking or execution bug introduced while making a pagination or warning-format change.
- **How to fix it:** Extract `ScanExecutor`, `ScanJobFactory`, `ScanRankingService`, and `ScanPresenter`.
- **Fix priority:** P1.

### CF-13
- **Severity:** Medium
- **Category:** correctness / product-contract mismatch
- **Location:** `src/backtestforecast/backtests/strategies/calendar.py`, `docs/backtest-strategy-assumptions.md`
- **Evidence:** Calendar spread now supports both call and put contracts through `strategy_overrides.calendar_contract_type`, but the default remains `call` for backward compatibility.
- **Why it is a problem:** Capability exists, but clients/templates that do not expose the override can still silently get call calendars.
- **Real-world failure mode:** A stale client or saved template submits `calendar_spread` without the override and receives the old call-calendar behavior.
- **How to fix it:** Ensure all clients/templates explicitly expose and persist the calendar contract type selection.
- **Fix priority:** P1.

### CF-14
- **Severity:** Medium
- **Category:** correctness
- **Location:** `docs/known-limitations.md`, naked-option strategies, margin logic
- **Evidence:** Naked options are explicitly documented as sized by margin requirement only and as understating theoretical risk.
- **Why it is a problem:** Collateral sufficiency is not the same as economic risk.
- **Real-world failure mode:** Backtests appear safer than the actual downside profile of naked options.
- **How to fix it:** Surface aggressive warnings in UI/API payloads and consider additional stress-loss sizing constraints.
- **Fix priority:** P1 for user-trust messaging, P2 for model redesign.

### CF-15
- **Severity:** Medium
- **Category:** correctness / analytics
- **Location:** `src/backtestforecast/services/backtests.py`, `docs/known-limitations.md`, config `RISK_FREE_RATE`
- **Evidence:** Risk-free rate is pulled from static config and persisted into runs, not dynamically matched to the historical date range.
- **Why it is a problem:** Sharpe/Sortino outputs are mechanically consistent but historically stale when rates move.
- **Real-world failure mode:** Users compare results across different periods and assume the risk-adjusted metrics are economically normalized when they are not.
- **How to fix it:** Either make the metric label explicit (“using configured static RFR”) or support date-aware Treasury-series lookup.
- **Fix priority:** P2.

### CF-16
- **Severity:** Medium
- **Category:** testing / ops
- **Location:** `.github/workflows/ci.yml`, `.github/workflows/cd.yml`, `tests/load/locustfile.py`
- **Evidence:** Load tests exist but are manual-only; CD uses health-check smoke tests rather than business workflow validation; no canary/weighted rollout.
- **Why it is a problem:** Performance and post-deploy behavioral regressions can ship undetected.
- **Real-world failure mode:** Production deploy passes health but fails checkout/create-backtest/export or degrades under moderate concurrency.
- **How to fix it:** Add lightweight staged load, post-deploy API smoke workflows, and canary rollout support.
- **Fix priority:** P1.

### CF-17
- **Severity:** Medium
- **Category:** security / ops
- **Location:** `docker-compose.prod.yml`
- **Evidence:** Redis is password-protected but not TLS-protected; compose comments push TLS to a future “if externalized” state.
- **Why it is a problem:** This is acceptable only on a single private host/network. It is not a safe default once topology changes.
- **Real-world failure mode:** Redis traffic exposed in plaintext after an infrastructure move that operators assume is “still production-ready.”
- **How to fix it:** Document topology assumptions loudly and provide a TLS-ready production template, not just comments.
- **Fix priority:** P2.

### CF-18
- **Severity:** Medium
- **Category:** frontend-backend mismatch
- **Location:** `docs/known-limitations.md`, `apps/web/lib/backtests/validation.ts`, `src/backtestforecast/schemas/backtests.py`
- **Evidence:** The docs still claim a UI/API mismatch on `target_dte`, but the current frontend constant is `1`, matching the backend schema.
- **Why it is a problem:** This is not a code defect now; it is a stale-contract defect.
- **Real-world failure mode:** Analysts/operators assume low-DTE requests are prevented when they are actually allowed.
- **How to fix it:** Remove the stale limitation note and add an explicit regression test around contract docs if docs are treated as operational truth.
- **Fix priority:** P1 because it changes product understanding.

### CF-19
- **Severity:** Low
- **Category:** dead code / misleading abstraction
- **Location:** large documentation surfaces plus previous audit docs
- **Evidence:** The repository contains multiple audit/open-items documents with overlapping but now divergent narratives.
- **Why it is a problem:** Engineers will cherry-pick the wrong source of truth.
- **Real-world failure mode:** Duplicate or contradictory follow-up work, especially around dispatch/outbox and schema/contract topics.
- **How to fix it:** Collapse audit history into one current status page and archive the rest as historical snapshots.
- **Fix priority:** P2.

### CF-20
- **Severity:** Medium
- **Category:** maintainability / hidden coupling
- **Location:** cross-cutting service + repository layers throughout backtests/scans/exports/billing
- **Evidence:** Domain services own both business rules and persistence/audit/event side effects. There is no clear command/query or orchestration/domain split.
- **Why it is a problem:** Side effects are hidden in “do everything” services, so reasoning about transaction boundaries requires reading entire files.
- **Real-world failure mode:** Subtle regressions in rollback, audit ordering, or status transitions.
- **How to fix it:** Introduce slimmer orchestration services and isolate pure domain logic from DB/event emission code.
- **Fix priority:** P2.

## 4. Frontend-Backend Contract Mismatches

1. **Pricing contract is not authoritative in the UI.** `apps/web/app/pricing/page.tsx` hardcodes amounts/features while the backend/Stripe checkout flow is authoritative. This is a customer-visible contract drift risk.
2. **Documentation falsely describes a `target_dte` mismatch.** Current frontend validation allows `target_dte >= 1`, matching backend schema, but docs still say the frontend enforces `>= 7`.
3. **Calendar-spread defaults can still be misleading.** The engine supports put calendars now, but clients that never send `strategy_overrides.calendar_contract_type` still get call calendars by default.
4. **Risk modeling for naked options is weaker than a typical user would infer from the generic strategy names.** The API returns “working” backtests, but the economic interpretation is looser than the name implies.
5. **Pricing/features shown on the marketing page can drift from Stripe env-backed reality without any code failure.** This is not caught by type generation or OpenAPI.

## 5. Database / Migration / Schema Drift Findings

- **No direct ORM/migration breakage was confirmed** from the source review; the repository has good migration discipline and explicit drift checks in CI.
- **Operational schema/documentation drift exists** around dispatch/outbox and target-DTE behavior, which is now more dangerous than schema drift because operators may act on stale assumptions.
- **Persistence-model risk still exists in billing audit durability.** State changes can commit while audit persistence fails.
- **Export storage model still has two behavior modes (DB blob vs S3 key) with materially different runtime characteristics.** The schema supports both, but operational behavior diverges sharply.

## 6. Workflow Trace Findings

### Auth flow
- Browser requests rely on Clerk bearer tokens, with cookie fallback for SSR/stateful flows.
- The dependency layer is careful about `Origin`/`Referer`/`X-Requested-With` checks for cookie auth.
- **Break points:** misleading docs, DB outages during auth-adjacent `/meta` behavior if stale docs are trusted, and the general complexity of dual-mode auth.

### Create flow
- Form submit → route validation → service enqueue → dispatch helper writes outbox + task metadata → commit → inline send → outbox poll fallback.
- **Break points:** dispatch docs are stale; operators can misdiagnose delayed jobs. Scan creation also computes expensive candidate counts before enqueue, increasing synchronous API latency risk.

### Update flow
- Most “updates” are status transitions in workers or billing sync via webhooks.
- **Break points:** giant services make it hard to reason about invariant ordering; billing audit failure is non-fatal and therefore loses evidence.

### Delete/archive flow
- Delete endpoints exist for backtests/exports/analysis/etc. and are rate-limited.
- **Break points:** storage cleanup and cascade-delete semantics are more complex than the API surface suggests; stale docs around storage/outbox reduce operator confidence.

### Background job flow
- API create → Celery task → worker service → state publish/persist → cleanup/reaper/outbox maintenance.
- **Break points:** worker task surface is too centralized; scan memory growth and sweep CPU inefficiency compete for the same worker resources.

### Export/import flow
- Export request creates job; worker renders CSV/PDF; API streams from S3 or DB blob.
- **Break points:** DB-backed download path loads full content into memory; large concurrent downloads can pressure API containers.

### Billing/entitlement flow
- UI checkout → Stripe checkout → webhook → user tier/subscription sync → entitlement checks across create endpoints.
- **Break points:** audit loss on DB failure, hardcoded pricing page contract drift, giant service complexity, and stale operational docs.

### Error/retry flow
- Good patterns exist: outbox, stale-job repair, CAS updates, retries in worker tasks.
- **Break points:** recovery logic is distributed across giant files; support/on-call burden rises because documentation is behind the implementation.

## 7. Performance Findings

1. **Scanner candidate accumulation in memory** — highest impact under broad scans; should be redesigned to O(K) memory.
2. **No in-flight coalescing for option contracts/quotes** — high impact under concurrent scans/sweeps/pipeline runs.
3. **CPU-bound genetic evaluation on thread pools** — high impact for sweep latency and queue starvation.
4. **DB-backed export downloads materialize full blobs** — moderate impact under concurrent downloads.
5. **Worker/domain god files slow safe iteration** — indirect but very real delivery-performance tax.
6. **Shared external-provider dependency across multiple heavy workflows** — not a code bug by itself, but the absence of stronger concurrency shaping means contention incidents are plausible.

## 8. Security Findings

Ranked by exploitability + blast radius:
1. **Billing audit durability gap** is not a classic exploit, but it is the largest security/compliance weakness because it undermines forensic reconstruction.
2. **Redis without TLS in production compose** is acceptable only within a tightly controlled single-host/private-network deployment. Unsafe if topology changes without compensating updates.
3. **Hardcoded pricing content** is a trust/security-adjacent issue because a customer can be induced to start checkout under stale commercial terms.
4. **Stale operational docs** materially degrade incident response for dispatch and auth-related behavior.
5. **No canary/real workflow validation in CD** raises blast radius for shipped security regressions.

## 9. Testing Gaps

- **Load tests are not part of CI/CD.** Risk left uncovered: concurrency/memory/provider-throttle incidents.
- **Post-deploy checks are too shallow.** Risk left uncovered: deploy passes health but fails real product actions.
- **Financial-model edge cases remain partly documented rather than fully simulated.** Risk left uncovered: user-visible “correct-looking” but wrong strategy outcomes.
- **Docs-vs-runtime invariants are untested.** Risk left uncovered: stale runbooks around outbox/dispatch/auth/product limits.
- **Large service refactors remain risky because tests prove behavior, but not clarity of ownership or blast-radius containment.**

## 10. Dead Code / Confusing Code / Refactor Targets

- Split `apps/worker/app/tasks.py`.
- Split `src/backtestforecast/services/billing.py`.
- Split `src/backtestforecast/services/scans.py`.
- Collapse stale audit docs and keep one current ops/audit status page.
- Replace pricing-page hardcoded data with backend-driven contract.
- Remove or rewrite stale “known limitations” entries that are no longer true.
- Ensure every client/template surface persists the calendar contract type explicitly.

## 11. Quick Wins

1. Update `docs/known-limitations.md` immediately.
2. Expose backend-driven pricing metadata and render pricing page from it.
3. Add in-flight dedupe for option contract/quote fetches.
4. Add durable fallback queue for billing audit failures.
5. Add staging smoke tests that create/read/delete one backtest/export/billing-ish contract call (non-destructive where possible).
6. Add explicit UI/API warnings for naked-option risk assumptions.
7. Add telemetry around scan candidate memory pressure and per-job candidate counts.
8. Add a docs-consistency checklist for dispatch/outbox/rate-limit/auth behavior.

## 12. Highest-Value Refactor Plan

### Immediate hotfixes
- Fix stale operational docs.
- Make pricing page backend-driven.
- Add billing-audit fallback persistence.
- Add option-fetch in-flight request coalescing.

### Short-term stabilization
- Rework scan candidate handling to bounded top-K memory.
- Add post-deploy workflow smoke tests.
- Add explicit modeling/UX warnings for naked options and static risk-free-rate behavior.
- Add alerting/metrics for export download memory and provider stampedes.

### Medium-term refactors
- Split worker tasks by domain.
- Split `BillingService` and `ScanService`.
- Convert wheel internals fully to `Decimal`.
- Add put-calendar or rename the strategy contract.

### Long-term architectural improvements
- Move heavy workflows toward clearer orchestration/domain separation.
- Add historical rate series for risk-adjusted metrics.
- Improve options modeling for assignment/exercise and other American-style events.
- Add canary/blue-green deployment and richer stage validation.

## 13. Appendix: File-by-File Notes

- `apps/api/app/main.py`: solid startup/runtime separation; dynamic config invalidation is well-documented; not a major concern.
- `apps/api/app/dependencies.py`: auth layer is more careful than average; cookie SSR flow is intentionally constrained.
- `apps/api/app/dispatch.py`: current dispatch/outbox behavior is materially better than old docs imply.
- `apps/api/app/routers/exports.py`: correct integrity checks, but DB-backed memory path remains.
- `apps/api/app/routers/events.py`: thoughtful SSE slot/process controls; still operationally sensitive to Redis capacity.
- `apps/web/app/pricing/page.tsx`: customer-visible hardcoded commercial contract; needs backend source of truth.
- `apps/worker/app/celery_app.py`: good explicit routing/scheduling; still depends on giant task module.
- `apps/worker/app/tasks.py`: too large; highest maintainability hotspot.
- `src/backtestforecast/backtests/engine.py`: good defensive structure, but missing early-assignment modeling remains material.
- `src/backtestforecast/backtests/strategies/calendar.py`: implementation narrower than strategy name suggests.
- `src/backtestforecast/backtests/strategies/wheel.py`: still precision-sensitive internally.
- `src/backtestforecast/billing/events.py`: audit durability gap.
- `src/backtestforecast/config.py`: wide config surface, generally strong validation, but product correctness still depends on operators maintaining env truth.
- `src/backtestforecast/exports/storage.py`: dual storage modes are practical but behaviorally divergent.
- `src/backtestforecast/market_data/service.py`: major throughput opportunity in coalescing option fetches.
- `src/backtestforecast/services/backtests.py`: good CAS/status patterns; metrics correctness still depends on simplifying financial assumptions.
- `src/backtestforecast/services/billing.py`: functional but too broad.
- `src/backtestforecast/services/exports.py`: acceptable caps, still memory-sensitive.
- `src/backtestforecast/services/scans.py`: biggest scaling hotspot.
- `src/backtestforecast/sweeps/genetic.py`: explicitly acknowledged CPU-parallelism limitation.
- `docs/known-limitations.md`: now partially misleading and should not be trusted until refreshed.
- `.github/workflows/ci.yml`: strong static/contract checks, but no automated load path.
- `.github/workflows/cd.yml`: sequential deployment with health checks only; no canary or workflow-level verification.
- `docker-compose.prod.yml`: reasonable container hardening, but Redis transport security remains topology-dependent.

---

## Top 100 fixes in exact implementation order

1. Rewrite `docs/known-limitations.md` to match current dispatch/outbox/`target_dte` reality.
2. Add one authoritative “current operational assumptions” page and archive stale audit docs.
3. Add backend pricing contract endpoint.
4. Replace `apps/web/app/pricing/page.tsx` hardcoded plans with fetched contract data.
5. Add regression test that pricing page values come from backend contract.
6. Add durable fallback persistence for failed billing audit writes.
7. Add a replay/drain task for deferred billing audit events.
8. Add alert on `billing.audit_write_failed`.
9. Add in-flight dedupe for `MassiveOptionGateway.list_contracts`.
10. Add in-flight dedupe for `MassiveOptionGateway.get_quote`.
11. Add metrics for coalesced vs duplicated option fetches.
12. Add provider-throttle alerting based on contract/quote retry volume.
13. Refactor scan execution to bounded top-K heap memory.
14. Persist scan candidates in batches/staging rather than one giant in-memory list.
15. Add scan memory-pressure metrics.
16. Add per-scan candidate histogram telemetry.
17. Convert wheel internal cash/equity tracking to `Decimal`.
18. Add regression tests for intermediate wheel equity-curve precision.
19. Implement early-assignment modeling for short-call strategies.
20. Add ex-dividend assignment warning surfaces in results.
21. Add tests for covered-call assignment around ex-dividend dates.
22. Add tests for naked-call assignment modeling.
23. Expose static-risk-free-rate usage explicitly in API/UI output.
24. Add optional historical rate-series support for Sharpe/Sortino.
25. Add UI warning for naked-option theoretical-risk understatement.
26. Add API warning code for naked-option backtests.
27. Propagate `calendar_contract_type` through every client/template/export surface and document the backward-compatible default.
28. Implement put-calendar support or split into explicit call/put strategies.
29. Update strategy catalog and docs for calendar semantics.
30. Split `apps/worker/app/tasks.py` into domain modules.
31. Keep task names/routing constants centralized after the split.
32. Split `BillingService` into focused services.
33. Split `ScanService` into orchestration/execution/ranking/presentation pieces.
34. Add explicit transaction-boundary documentation per service.
35. Add post-deploy smoke test for authenticated backtest create/list/detail.
36. Add post-deploy smoke test for export create/status/download.
37. Add post-deploy smoke test for pricing/billing contract fetch.
38. Add post-deploy smoke test for webhook signature path in staging.
39. Add lightweight CI load test run.
40. Add staged load test before production promotion.
41. Add canary or weighted deployment support.
42. Add richer rollback automation than health-only failure.
43. Add Redis TLS-ready production template, not just comments.
44. Enforce topology-specific Redis security docs in deploy checklist.
45. Add explicit S3-first policy for exports above a small threshold.
46. Add true streaming DB export read path or remove DB-blob downloads for large files.
47. Add concurrency caps or queue partitioning for heavy sweeps.
48. Refactor sweep fitness evaluation for process-based parallelism.
49. Add metrics for sweep CPU efficiency / queue wait time.
50. Add stronger worker autoscaling signals from queue depth + duration.
51. Add documentation CI lint for stale invariants around dispatch/outbox/rate limits.
52. Add an owner for operational docs.
53. Remove contradictory historical audit docs from primary navigation.
54. Add product copy clarifying that checkout is authoritative only temporarily until pricing contract is live.
55. Add support ticket playbook for pricing mismatch response.
56. Add audit log replay tooling for Stripe disputes.
57. Add monthly validation that `RISK_FREE_RATE` is current.
58. Add metrics dimension for low-DTE run submissions.
59. Audit every frontend marketing claim against entitlement code.
60. Add a generated contract for plan features/limits.
61. Use that generated contract in pricing page, dashboard warnings, and upgrade prompts.
62. Add tests ensuring docs mention outbox only when outbox scheduling exists.
63. Add tests ensuring `target_dte` docs match validation constants.
64. Add health/readiness output documenting outbox enabled state.
65. Add operator alert when outbox pending count grows.
66. Add operator alert when stale queued jobs exceed SLA.
67. Add scan queue isolation or dedicated worker pool if traffic grows.
68. Add sweep queue isolation or dedicated worker pool if traffic grows.
69. Add provider budget/rate-limit accounting per workflow type.
70. Add result labeling when a model uses simplified financial assumptions.
71. Add strategy-specific disclaimers to exports/PDFs.
72. Add path-dependent metric validation for wheel and assignment-sensitive strategies.
73. Add stronger data freshness labels for option-cache-backed results.
74. Add explicit “model version / assumptions version” to backtest output.
75. Version pricing contract responses.
76. Version strategy assumption surfaces.
77. Add UI badge when a strategy uses approximation-heavy modeling.
78. Add backtest engine integration suite for dividend-sensitive equities.
79. Add profitability-delta comparison tests between static and historical RFR modes.
80. Add concurrency benchmarks for option data cache stampede scenarios.
81. Benchmark scan memory before/after bounded heap refactor.
82. Benchmark export download memory before/after streaming changes.
83. Benchmark sweep throughput before/after process-pool change.
84. Add repository/service ownership map in docs.
85. Make service side effects explicit in docstrings.
86. Add tracing spans around scan candidate evaluation and provider fetch duplication.
87. Add tracing around billing webhook claim → apply → audit sequence.
88. Add tracing around export storage mode selection.
89. Add contract tests for pricing + entitlement consistency.
90. Add integration test proving outbox docs and scheduler are aligned.
91. Add E2E test covering pricing page → checkout session creation.
92. Add E2E test covering a low-DTE backtest submission from the web UI if intentionally allowed.
93. Add product decision note for whether sub-weekly DTE should remain allowed.
94. Add dashboards for scan memory, export download bytes, sweep queue delay.
95. Add runbook for provider stampede mitigation.
96. Add runbook for billing audit replay.
97. Add runbook for stale docs cleanup ownership.
98. Reduce giant-file line counts with enforced thresholds or warnings.
99. Document subsystem ownership across API/web/worker/shared package.
100. Re-audit the system after these fixes; many current risks are coupling-driven and will only disappear after structure changes.

## Top 20 bugs most likely already affecting production

1. Scan jobs consuming more memory than necessary due to candidate accumulation.
2. Duplicate contract/quote fetches under concurrent workloads.
3. Missing billing audit records during transient DB failures.
4. Covered-call results overstated near ex-dividend dates.
5. Wheel equity curves drifting slightly from fully precise accounting.
6. Marketing/pricing page drifting from Stripe-configured commercial reality.
7. Operators/support using stale dispatch/outbox documentation.
8. DB-backed export downloads causing avoidable API memory spikes.
9. Sweep jobs underusing available CPU because of thread-bound fitness evaluation.
10. Scan regressions hiding inside oversized `ScanService` changes.
11. Billing regressions hiding inside oversized `BillingService` changes.
12. Task-routing or maintenance regressions hiding inside oversized worker task file changes.
13. Static `RISK_FREE_RATE` producing stale risk-adjusted metrics.
14. Older clients/templates silently falling back to call calendars because they omit `calendar_contract_type`.
15. Users misinterpreting naked-option results as full-risk-aware modeling.
16. Production deploys passing health while failing meaningful workflows.
17. Redis transport assumptions becoming unsafe after topology changes.
18. Export storage-mode behavior differing from operator expectations.
19. Documentation causing false assumptions about low-DTE request prevention.
20. Shared provider contention affecting multiple asynchronous subsystems at once.

## Top 20 things that look correct but may be silently wrong

1. Covered-call hold-to-expiration results on dividend-paying names.
2. Naked-call backtests that appear conservative because margin sizing “works.”
3. Wheel equity curves that visually look right despite float-path drift.
4. Sharpe/Sortino values that are numerically stable but economically stale.
5. Pricing page values that look polished but may not match checkout reality.
6. Calendar spread results from stale clients/templates that still omit `calendar_contract_type` and therefore silently mean call calendars.
7. Export download path that works in tests but scales poorly under concurrency.
8. Dispatch troubleshooting guided by docs rather than current code.
9. Low-DTE assumptions held by support/ops because of stale docs.
10. Product/commercial promises inferred from UI strings instead of backend contracts.
11. Scan throughput assumptions based on worker CPU count despite thread-bound GA sections.
12. Provider-cache hit assumptions that ignore in-flight duplication.
13. Audit completeness assumptions based on billing success.
14. Safety assumptions around Redis transport when infrastructure changes.
15. Confidence in giant service files because tests pass locally.
16. “Feature complete” interpretation of strategy catalog entries that hide approximation-heavy implementations.
17. Backtest summary trust in strategies with documented but easy-to-miss modeling shortcuts.
18. Deployment safety confidence based on health checks only.
19. Operational confidence in historical audit docs that are no longer current.
20. Perceived maturity from large test count despite unresolved coupling/scaling issues.
