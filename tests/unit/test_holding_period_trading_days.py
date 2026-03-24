"""Test that holding_period_trading_days is included in the trade bulk INSERT.

Regression test for the bug where the engine computed trading days held
but the service layer omitted the field during persistence, causing it
to always read back as NULL from the database.
"""
from __future__ import annotations

import inspect


def test_trade_insert_includes_holding_period_trading_days():
    """The trade dict builder must include holding_period_trading_days."""
    import backtestforecast.services.backtests as module

    source = inspect.getsource(module)
    assert '"holding_period_trading_days"' in source or "'holding_period_trading_days'" in source or "holding_period_trading_days" in source, (
        "backtests service must include holding_period_trading_days "
        "in the trade bulk insert dict."
    )

    lines = source.splitlines()
    in_trade_dict = False
    found = False
    for line in lines:
        if "trade_dicts.append" in line:
            in_trade_dict = True
        if in_trade_dict and "holding_period_trading_days" in line:
            found = True
            break
        if in_trade_dict and "})" in line.strip():
            in_trade_dict = False

    assert found, (
        "holding_period_trading_days must appear inside the trade_dicts.append({...}) "
        "block in BacktestService. Without it, the engine-computed value is silently lost."
    )
