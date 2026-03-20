"""Comprehensive beat schedule validation.

Verifies that all expected periodic tasks are registered in the beat
schedule, their task references resolve to real task names, and their
queue routing is correct.
"""
from __future__ import annotations

import pytest


EXPECTED_BEAT_ENTRIES = {
    "refresh-prioritized-scans-daily",
    "nightly-scan-pipeline",
    "reap-stale-jobs",
    "reconcile-s3-orphans-daily",
    "cleanup-audit-events-weekly",
    "refresh-market-holidays-weekly",
    "cleanup-daily-recommendations-weekly",
    "cleanup-outbox-daily",
    "poll-outbox",
    "reconcile-subscriptions-daily",
    "expire-old-exports",
}


@pytest.fixture(scope="module")
def beat_schedule() -> dict:
    from apps.worker.app.celery_app import celery_app
    return dict(celery_app.conf.beat_schedule)


@pytest.fixture(scope="module")
def registered_task_names() -> set[str]:
    from apps.worker.app import tasks as _  # noqa: F841 — force task registration
    from apps.worker.app.celery_app import celery_app
    return set(celery_app.tasks.keys())


def test_all_expected_entries_exist(beat_schedule: dict) -> None:
    """Every expected periodic task must be present in the beat schedule."""
    actual = set(beat_schedule.keys())
    missing = EXPECTED_BEAT_ENTRIES - actual
    assert not missing, f"Beat schedule is missing entries: {sorted(missing)}"


def test_no_unexpected_entries(beat_schedule: dict) -> None:
    """Alert when new beat entries are added but not listed in EXPECTED_BEAT_ENTRIES."""
    actual = set(beat_schedule.keys())
    unexpected = actual - EXPECTED_BEAT_ENTRIES
    if unexpected:
        pytest.fail(
            f"New beat schedule entries found: {sorted(unexpected)}. "
            "Add them to EXPECTED_BEAT_ENTRIES in this test."
        )


def test_all_beat_tasks_are_registered(beat_schedule: dict, registered_task_names: set[str]) -> None:
    """Every task referenced by a beat entry must be a registered Celery task."""
    unresolved = []
    for entry_name, config in beat_schedule.items():
        task_name = config.get("task", "")
        if task_name not in registered_task_names:
            unresolved.append((entry_name, task_name))
    assert not unresolved, (
        f"Beat schedule references unregistered tasks: "
        f"{[(e, t) for e, t in unresolved]}"
    )


def test_beat_entries_have_schedules(beat_schedule: dict) -> None:
    """Every beat entry must have a 'schedule' key."""
    missing_schedule = [
        name for name, config in beat_schedule.items()
        if "schedule" not in config
    ]
    assert not missing_schedule, f"Beat entries without schedule: {missing_schedule}"


def test_maintenance_tasks_routed_to_maintenance_queue() -> None:
    """All maintenance.* tasks must be routed to the 'maintenance' queue."""
    from apps.worker.app.celery_app import celery_app
    routing = celery_app.conf.task_routes or {}
    for task_name, route in routing.items():
        if task_name.startswith("maintenance."):
            queue = route.get("queue") if isinstance(route, dict) else route
            assert queue == "maintenance", (
                f"Task {task_name} should be routed to 'maintenance' queue, "
                f"got {queue!r}"
            )
