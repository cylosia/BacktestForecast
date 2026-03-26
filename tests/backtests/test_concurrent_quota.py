"""Verify quota enforcement uses SELECT FOR UPDATE to prevent races."""
from __future__ import annotations


def test_enforce_quota_uses_for_update():
    """Shared quota enforcement must lock the user row."""
    import inspect

    from backtestforecast.services.backtest_workflow_access import enforce_backtest_workflow_quota
    source = inspect.getsource(enforce_backtest_workflow_quota)
    assert "with_for_update" in source, "Quota enforcement must use SELECT FOR UPDATE"
