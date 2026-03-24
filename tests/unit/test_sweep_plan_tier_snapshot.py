"""Verify SweepService.create_job sets plan_tier_snapshot from user.plan_tier."""
from __future__ import annotations

import inspect

from backtestforecast.services.sweeps import SweepService


def test_create_job_sets_plan_tier_snapshot():
    """The SweepJob constructor in create_job must include plan_tier_snapshot=user.plan_tier."""
    source = inspect.getsource(SweepService.create_job)
    assert "plan_tier_snapshot" in source, (
        "SweepService.create_job does not set plan_tier_snapshot - "
        "all sweep jobs will default to 'free' regardless of user tier."
    )
    assert "user.plan_tier" in source, (
        "SweepService.create_job references plan_tier_snapshot but does not "
        "derive it from user.plan_tier."
    )
