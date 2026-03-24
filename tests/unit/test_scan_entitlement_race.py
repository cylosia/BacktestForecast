"""Fix 76: Scan service acquires row lock on User before checking entitlements.

Verifies that ScanService.create_job uses SELECT ... FOR UPDATE on the
User row to prevent race conditions when checking entitlements.
"""
from __future__ import annotations

import inspect


class TestScanEntitlementRace:
    """Verify ScanService.create_job locks the User row."""

    def test_create_job_uses_for_update_on_user(self):
        """create_job must SELECT the User row with FOR UPDATE."""
        from backtestforecast.services.scans import ScanService

        source = inspect.getsource(ScanService._create_job_impl)
        assert "with_for_update" in source, (
            "create_job must use with_for_update() on User to prevent "
            "concurrent entitlement check races"
        )

    def test_for_update_precedes_policy_resolution(self):
        """The FOR UPDATE lock must be acquired before resolving scanner policy."""
        from backtestforecast.services.scans import ScanService

        source = inspect.getsource(ScanService._create_job_impl)
        lock_idx = source.find("with_for_update")
        policy_idx = source.find("resolve_scanner_policy")
        assert lock_idx >= 0, "Must use with_for_update()"
        assert policy_idx >= 0, "Must call resolve_scanner_policy()"
        assert lock_idx < policy_idx, (
            "FOR UPDATE lock must be acquired before resolving scanner policy"
        )

    def test_for_update_targets_user_table(self):
        """The locked query must target the User model, not ScannerJob."""
        from backtestforecast.services.scans import ScanService

        source = inspect.getsource(ScanService._create_job_impl)
        lines_before_lock = source[: source.find("with_for_update")]
        last_select = lines_before_lock.rfind("select(")
        snippet = lines_before_lock[last_select : last_select + 50]
        assert "User" in snippet, (
            "The FOR UPDATE query must target the User model"
        )

    def test_session_execute_called_for_lock(self):
        """The locked select must be executed via session.execute()."""
        from backtestforecast.services.scans import ScanService

        source = inspect.getsource(ScanService._create_job_impl)
        assert "self.session.execute" in source, (
            "Locked query must be executed via self.session.execute()"
        )
