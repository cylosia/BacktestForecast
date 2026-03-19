"""Verify backtest engine uses Decimal for cash accumulation."""
from decimal import Decimal


def test_engine_uses_decimal_for_cash():
    """The engine should use Decimal, not float, for cash tracking."""
    import inspect
    from backtestforecast.backtests.engine import OptionsBacktestEngine

    source = inspect.getsource(OptionsBacktestEngine.run)
    assert "Decimal" in source, "Engine.run should use Decimal for cash accumulation"
    assert "float(config.account_size)" not in source, "Engine should not convert account_size to float"
