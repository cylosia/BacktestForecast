"""Verify quota enforcement uses SELECT FOR UPDATE to prevent races."""
from __future__ import annotations


def test_enforce_quota_uses_for_update():
    """The _enforce_backtest_quota method must lock the user row."""
    from backtestforecast.services.backtests import BacktestService
    import inspect
    source = inspect.getsource(BacktestService._enforce_backtest_quota)
    assert "with_for_update" in source, "Quota enforcement must use SELECT FOR UPDATE"
