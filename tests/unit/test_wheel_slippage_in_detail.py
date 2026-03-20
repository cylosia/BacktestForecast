"""Test that wheel strategy trade results include slippage in detail_json.

The main engine stores total_slippage, entry_slippage, exit_slippage in
each trade's detail_json. The wheel engine must do the same so consumers
can reconcile gross_pnl - total_commissions - total_slippage = net_pnl.
"""
from __future__ import annotations

import inspect

from backtestforecast.backtests.strategies.wheel import WheelStrategyBacktestEngine


def test_wheel_engine_stores_slippage_in_detail_json() -> None:
    """The wheel engine's run method must include total_slippage in detail_json."""
    source = inspect.getsource(WheelStrategyBacktestEngine.run)
    assert source.count('"total_slippage"') >= 4, (
        "WheelEngine.run should store 'total_slippage' in detail_json for "
        "all trade result paths (non-assignment, put-assignment, call-assignment, "
        "backtest-end, stock liquidation)"
    )
    assert '"entry_slippage"' in source
    assert '"exit_slippage"' in source
