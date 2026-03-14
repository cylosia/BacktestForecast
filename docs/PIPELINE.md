# Pipeline Behavior

## Nightly Pipeline: Stale-Run Stomping

### Overview

The nightly pipeline includes stale-run detection to prevent runs that are stuck in "running" or "queued" from blocking new work indefinitely.

### Stale-Run Detection

Runs older than **1 hour** in `running` or `queued` state are marked as `failed`.

### Age Check

The 1-hour threshold exists to avoid **stomping on legitimate runs**:

- Backtests and scans can legitimately run for 30–45 minutes on complex strategies or large date ranges
- A shorter threshold (e.g., 15 minutes) would incorrectly fail runs that are still progressing
- 1 hour provides a buffer for normal long-running jobs while catching truly stuck runs (e.g., worker crash, broker disconnect)

### Implementation Notes

- The reaper task (`maintenance.reap_stale_jobs`) runs periodically (e.g., every 10 minutes via Celery Beat)
- Only runs with `started_at` (or equivalent timestamp) older than 1 hour are affected
- Once marked `failed`, the run is no longer considered active; new pipeline work can proceed
