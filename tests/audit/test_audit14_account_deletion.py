"""Tests verifying account deletion safety.

Testing Gaps:
  TG-7: Account deletion + concurrent worker commit

Verifies that account deletion:
1. Calls cancel_in_flight_jobs (which flushes DB)
2. Commits cancellations before deleting the user
3. Wraps cancel_in_flight_jobs in try/except so deletion proceeds on failure
"""
from __future__ import annotations

import inspect


class TestAccountDeletionSafety:
    def test_cancel_in_flight_called_before_delete(self):
        from apps.api.app.routers.account import delete_account
        src = inspect.getsource(delete_account)
        cancel_pos = src.find("cancel_in_flight_jobs")
        delete_pos = src.find("db.delete(user)")
        assert cancel_pos > 0 and delete_pos > 0
        assert cancel_pos < delete_pos, (
            "cancel_in_flight_jobs must be called before db.delete(user)"
        )

    def test_cancel_wrapped_in_try_except(self):
        from apps.api.app.routers.account import delete_account
        src = inspect.getsource(delete_account)
        assert "cancel_in_flight_failed" in src or "except Exception" in src

    def test_two_phase_commit(self):
        """Commit cancellations separately from the user delete."""
        from apps.api.app.routers.account import delete_account
        src = inspect.getsource(delete_account)
        commits = [i for i in range(len(src)) if src[i:i+11] == "db.commit()"]
        assert len(commits) >= 2, (
            f"Expected at least 2 db.commit() calls (cancel+audit, then delete), "
            f"got {len(commits)}"
        )
