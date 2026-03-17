"""Verify margin warning only fires when entry is accepted."""
import inspect
from backtestforecast.backtests.engine import OptionsBacktestEngine


def test_margin_warning_inside_else_branch():
    """The margin_reserved warning must only fire when the entry is accepted
    (inside the else branch), not when it's rejected due to negative cash."""
    source = inspect.getsource(OptionsBacktestEngine.run)
    # Find the negative_cash_rejected block and the margin_reserved block.
    # The margin block must appear AFTER "position = candidate" (inside else)
    # and must be indented deeper than the if/else block.
    neg_cash_idx = source.find("negative_cash_rejected")
    position_set_idx = source.find("position = candidate", neg_cash_idx)
    margin_idx = source.find("margin_reserved", position_set_idx)
    assert neg_cash_idx < position_set_idx < margin_idx, (
        "margin_reserved warning must appear after 'position = candidate'"
    )
