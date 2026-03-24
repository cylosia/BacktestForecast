"""Verify that all expected Celery tasks are registered.

Imports the worker's Celery application and asserts every task name
referenced by routers, Beat schedule, and the reaper is present in the
task registry.  Exit code 1 if any are missing.
"""
from __future__ import annotations

import sys

EXPECTED_TASKS = [
    "backtests.run",
    "exports.generate",
    "scans.run_job",
    "scans.refresh_prioritized",
    "analysis.deep_symbol",
    "pipeline.nightly_scan",
    "maintenance.reap_stale_jobs",
    "maintenance.ping",
    "sweeps.run",
]


def main() -> int:
    # Force task module import so decorators register
    import apps.worker.app.tasks  # noqa: F401
    from apps.worker.app.celery_app import celery_app

    registered = set(celery_app.tasks.keys())
    missing = [name for name in EXPECTED_TASKS if name not in registered]

    if missing:
        print(f"MISSING {len(missing)} Celery task(s):")
        for name in missing:
            print(f"  - {name}")
        return 1

    print(f"OK - all {len(EXPECTED_TASKS)} expected tasks are registered.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
