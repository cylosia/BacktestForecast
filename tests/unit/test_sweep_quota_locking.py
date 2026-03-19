"""Fix 63: _enforce_sweep_quota acquires a FOR UPDATE lock on the User row.

Verify that the method serializes concurrent quota checks by issuing a
SELECT ... FOR UPDATE on the User table before counting sweeps.
"""
from __future__ import annotations

import inspect
import textwrap

from backtestforecast.services.sweeps import SweepService


class TestSweepQuotaLocking:
    def test_enforce_sweep_quota_uses_for_update(self):
        """The query in _enforce_sweep_quota must use .with_for_update()."""
        source = inspect.getsource(SweepService._enforce_sweep_quota)
        assert "with_for_update" in source, (
            "_enforce_sweep_quota must use .with_for_update() to serialize "
            "concurrent quota checks"
        )

    def test_enforce_sweep_quota_selects_user(self):
        """The FOR UPDATE lock must target the User model."""
        source = inspect.getsource(SweepService._enforce_sweep_quota)
        assert "User" in source, (
            "_enforce_sweep_quota must lock the User row"
        )
        assert "select(User)" in source, (
            "_enforce_sweep_quota must SELECT the User row for locking"
        )

    def test_enforce_sweep_quota_checks_monthly_usage(self):
        """The method must count sweeps created in the current month."""
        source = inspect.getsource(SweepService._enforce_sweep_quota)
        assert "SweepJob" in source, (
            "_enforce_sweep_quota must query SweepJob for monthly usage"
        )
        assert "month" in source.lower(), (
            "_enforce_sweep_quota must reference monthly boundaries"
        )

    def test_enforce_sweep_quota_checks_concurrent_sweeps(self):
        """The method must check for concurrent running/queued sweeps."""
        source = inspect.getsource(SweepService._enforce_sweep_quota)
        assert "running" in source, (
            "_enforce_sweep_quota must check for running sweeps"
        )
        assert "queued" in source, (
            "_enforce_sweep_quota must check for queued sweeps"
        )
