"""Verify backtest engine uses Decimal for cash accumulation."""


def test_engine_uses_decimal_for_cash():
    """The engine should use Decimal, not float, for cash tracking."""
    import inspect

    from backtestforecast.backtests.engine import OptionsBacktestEngine

    source = inspect.getsource(OptionsBacktestEngine.run)
    assert "Decimal" in source, "Engine.run should use Decimal for cash accumulation"
    assert 'cash = Decimal(str(config.account_size))' in source, (
        "Engine.run should initialize cash as Decimal from config.account_size"
    )
    assert "ending_equity_f = float(equity_curve[-1].equity)" in source, (
        "Engine.run may convert to float at the summary/statistics boundary"
    )
