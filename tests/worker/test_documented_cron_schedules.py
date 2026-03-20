from __future__ import annotations

from pathlib import Path

from celery.schedules import crontab

from apps.worker.app.celery_app import celery_app

README_PATH = Path(__file__).resolve().parents[2] / "README.md"

DOCUMENTED_CRON_SCHEDULES = {
    "nightly pipeline": {
        "beat_entry": "nightly-scan-pipeline",
        "readme_snippet": "Pipeline runs nightly at 6:00 AM UTC via Celery beat on the `pipeline` queue",
        "expected": {"minute": {0}, "hour": {6}},
    },
    "daily recommendations cleanup": {
        "beat_entry": "cleanup-daily-recommendations-weekly",
        "readme_snippet": None,
        "expected": {"minute": {30}, "hour": {2}, "day_of_week": {0}},
    },
}


def _cron_field_values(field: object) -> set[int]:
    if isinstance(field, set):
        return {int(value) for value in field}
    return {int(value) for value in getattr(field, "values", set())}


def test_documented_cron_schedules_match_celery_beat_config() -> None:
    readme = README_PATH.read_text()
    beat_schedule = celery_app.conf.beat_schedule

    for label, expectation in DOCUMENTED_CRON_SCHEDULES.items():
        beat_entry = expectation["beat_entry"]
        assert beat_entry in beat_schedule, f"Missing beat entry for {label}: {beat_entry}"

        schedule = beat_schedule[beat_entry]["schedule"]
        assert isinstance(schedule, crontab), f"Expected crontab schedule for {label}"

        for field_name, expected_values in expectation["expected"].items():
            actual_values = _cron_field_values(getattr(schedule, field_name))
            assert actual_values == expected_values, (
                f"{label} drifted: expected {field_name}={sorted(expected_values)}, "
                f"got {sorted(actual_values)}"
            )

        snippet = expectation["readme_snippet"]
        if snippet:
            assert snippet in readme, f"README is missing the documented schedule for {label}"
