# Data provider outage strategy

## Goals
- Preserve existing user data and history access.
- Avoid implying certainty or stale execution results.
- Fail safely without corrupting scanner/backtest state.

## Immediate behavior
- Manual backtest creation and scan execution should fail fast with a clear provider-unavailable message after retries are exhausted.
- History, billing, pricing, and exported-result downloads remain available.
- `/health/ready` should show `redis` and `database`; provider health should be monitored externally and surfaced on dashboards.

## Response playbook
1. Detect elevated Massive 429/5xx or timeout rates.
2. Pause marketing pushes and scheduled refreshes if backlog grows.
3. Keep the UI read-only for existing results; optionally disable new launch buttons via feature flag.
4. Communicate provider degradation in-app and on status page.
5. Resume scheduled refreshes only after provider error rate stabilizes.

## Medium-term resilience
- Add cached daily-bar snapshots for the most common symbols.
- Add provider status metrics to dashboards.
- Introduce a secondary market-data source behind a provider interface before broad launch.
