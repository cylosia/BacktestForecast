"""Contract test: verify critical enum values are consistent."""
from backtestforecast.schemas.common import JobStatus, PlanTier


def test_plan_tier_values():
    expected = {"free", "pro", "premium"}
    actual = {t.value for t in PlanTier}
    assert actual == expected, f"PlanTier drift: {actual} != {expected}"


def test_job_status_values():
    expected = {"queued", "running", "succeeded", "failed", "cancelled", "expired"}
    actual = {s.value for s in JobStatus}
    assert actual == expected, f"JobStatus drift: {actual} != {expected}"
