# Support Runbook: Stuck Jobs

## When to use this
- A backtest, export, scan, sweep, or analysis stays `queued` or `running` longer than expected.
- A user reports that cancel/delete did not resolve the job.
- Alerts such as `StuckJobsHigh`, `ScannerJobStuckRunning`, or `SweepJobStuckRunning` are firing.

## First checks
1. Confirm the job type, job ID, user ID, and current status.
2. Check whether the job has a visible cancel endpoint and ask the user to cancel if the job is still active.
3. Check `/health/ready` or `/admin/dlq` for `queue_diagnostics`, especially `stale_queued_total` and `stale_without_outbox_total`.
4. Check worker health and queue backlog before assuming the job row is corrupt.

## Per-job remediation
1. If the job is `queued` or `running`, use the public cancel endpoint first:
   - `POST /v1/backtests/{run_id}/cancel`
   - `POST /v1/exports/{export_job_id}/cancel`
   - `POST /v1/scans/{job_id}/cancel`
   - `POST /v1/sweeps/{job_id}/cancel`
   - `POST /v1/analysis/{analysis_id}/cancel`
2. If cancellation succeeds, delete only after the job reaches `cancelled` when the product supports delete-after-cancel.
3. If cancellation fails because the worker is already gone or the broker is unhealthy, trigger `maintenance.reap_stale_jobs`.

## Operator escalation
1. Check worker logs for the specific task name and job ID.
2. Check whether the job has a stale `celery_task_id` or missing outbox record.
3. If queue diagnostics indicate stranded queued jobs, run `python scripts/repair_stranded_jobs.py --action list` before any manual SQL.
4. If manual intervention is still required, record the exact SQL or admin action in the incident ticket.

## User-facing guidance
- Do not tell the user to retry blindly if the original job is still active.
- Prefer cancel, then re-run.
- If a job was manually repaired or force-failed, tell the user the original job ID was retired and a new run may be needed.
