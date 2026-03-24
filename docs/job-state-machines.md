# Job State Machines

This document is the source of truth for async job lifecycle expectations across the API, services, workers, and operational tooling.

## Shared Rules

- Initial state: `queued`
- Active states: `queued`, `running`
- Terminal states:
  - most jobs: `succeeded`, `failed`, `cancelled`
  - exports also allow `expired`
- Delete is only allowed from a terminal state.
- Cancel is only allowed from an active state.
- A terminal job must never transition back to `running`.
- Worker redelivery may reclaim ownership only while the job is still non-terminal.

## Backtest Runs

States:
- `queued`
- `running`
- `succeeded`
- `failed`
- `cancelled`

Allowed transitions:
- `queued -> running`
- `queued -> cancelled`
- `queued -> failed`
- `running -> succeeded`
- `running -> failed`
- `running -> cancelled`

Operational notes:
- Summary metrics are read from persisted run aggregates, not recomputed from returned trade slices.
- Worker retries may re-enter while status is `running`, but they must not overwrite terminal status.

## Export Jobs

States:
- `queued`
- `running`
- `succeeded`
- `failed`
- `cancelled`
- `expired`

Allowed transitions:
- `queued -> running`
- `queued -> cancelled`
- `queued -> failed`
- `running -> succeeded`
- `running -> failed`
- `running -> cancelled`
- `succeeded -> expired`

Operational notes:
- Storage cleanup is best-effort after DB transitions.
- `expired` is terminal and must not re-enter generation.

## Scanner Jobs

States:
- `queued`
- `running`
- `succeeded`
- `failed`
- `cancelled`

Allowed transitions:
- `queued -> running`
- `queued -> cancelled`
- `queued -> failed`
- `running -> succeeded`
- `running -> failed`
- `running -> cancelled`

Operational notes:
- Candidate persistence must be guarded by the running-state CAS.
- Partial recommendation payloads must not rewrite persisted summary truth.

## Sweep Jobs

States:
- `queued`
- `running`
- `succeeded`
- `failed`
- `cancelled`

Allowed transitions:
- `queued -> running`
- `queued -> cancelled`
- `queued -> failed`
- `running -> succeeded`
- `running -> failed`
- `running -> cancelled`

Operational notes:
- Result persistence must be guarded by the running-state CAS.
- Quota and entitlement failures should fail the job with an actionable error message.

## Deep Analyses

States:
- `queued`
- `running`
- `succeeded`
- `failed`
- `cancelled`

Allowed transitions:
- `queued -> running`
- `queued -> cancelled`
- `queued -> failed`
- `running -> succeeded`
- `running -> failed`
- `running -> cancelled`

Stage progression while `running`:
- `pending -> regime -> landscape -> deep_dive -> forecast`

Operational notes:
- Stage progression is monotonic.
- Concurrent-limit failures should tell the user to wait or cancel an active analysis.

## Worker Resource Ownership

Workers own and must close the resources they instantiate inside task scope:

- task-scoped services such as `BacktestService`, `ExportService`, `ScanService`, `SweepService`
- task-scoped adapters/executors such as deep-analysis executors
- task-scoped market-data services and Massive clients

Shared rule:
- if a task created it, the task is responsible for closing it in its `finally` path
- helper: `apps.worker.app.task_helpers.close_owned_resource`
