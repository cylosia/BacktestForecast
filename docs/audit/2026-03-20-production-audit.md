# 2026-03-20 Production Audit Notes

This document records the concrete findings identified during a manual production-grade audit.

## Highest-impact confirmed defects

1. `apps/api/app/routers/account.py` defines `_GdprPagination` with a required `hint` field and only `backtests_offset`, but `export_account_data()` returns no `hint` and several extra offsets (`templates_offset`, `scans_offset`, `sweeps_offset`, `exports_offset`, `analyses_offset`, `audit_offset`). This can cause response-model validation failure or silent field loss in the GDPR export endpoint.
2. `apps/web/lib/backtests/validation.ts` and `apps/web/components/backtests/ta-rule-controls.tsx` use support/resistance values `support` / `resistance`, while the backend enum only accepts `near_support`, `near_resistance`, `breakout_above_resistance`, and `breakdown_below_support`.
3. `apps/web/lib/templates/parse.ts` does not recognize `custom_7_leg`, even though the backend exposes it in `StrategyType` and entitlement policy.
4. `apps/web/lib/templates/parse.ts` only rehydrates RSI and moving-average rules; every other backend-supported rule type is silently dropped when applying a template.
5. `apps/web/lib/scanner/constants.ts` hard-codes the maximum scan window to 730 days even though the backend limit is configuration-driven (`Settings.max_scanner_window_days`).
6. `apps/web/app/pricing/page.tsx` hard-codes display prices instead of reading configured Stripe price metadata, creating guaranteed drift risk.
7. `apps/api/app/routers/events.py` never emits SSE event IDs, but `apps/web/hooks/use-sse.ts` stores `lastEventId` and reconnects with `?lastEventId=...`; resume semantics are not implemented.
8. `src/backtestforecast/services/backtests.py` marks `equity_curve_truncated` true when `len(equity_points) >= EQUITY_CURVE_LIMIT`, so a run with exactly the limit is misreported as truncated.
9. `src/backtestforecast/security/http.py` configures request-body overrides for `/v1/events/backtest`, `/v1/events/scan`, etc., but the actual SSE endpoints are pluralized (`/v1/events/backtests/...`, `/v1/events/scans/...`, ...), so the overrides are dead code.
10. `README.md` claims the free plan comparison limit is effectively 1 while `FeaturePolicy` enforces 2; operator/user-facing documentation is stale.

## Additional observations

- The repo has extensive tests, but the above frontend/backend drift issues still shipped, which means current contract coverage does not sufficiently exercise real browser form payloads and template application behavior.
- Several comments acknowledge known orphan-risk or drift-risk scenarios (`pricing/page.tsx`, export storage comments), which is useful, but those comments do not mitigate the production impact.
