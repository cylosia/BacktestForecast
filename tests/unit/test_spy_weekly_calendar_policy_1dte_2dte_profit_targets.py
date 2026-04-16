from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from scripts.grid_search_weekly_calendar_policy_two_stage_spy_1dte_2dte import (  # noqa: E402
    _build_strategy_sets,
    _parse_profit_targets,
    _profit_target_suffix,
)


def test_parse_profit_targets_accepts_csv() -> None:
    assert _parse_profit_targets("5, 10,15") == (5, 10, 15)


def test_profit_target_suffix_formats_output_suffix() -> None:
    assert _profit_target_suffix((5, 10, 15)) == "_pt5_10_15"


def test_build_strategy_sets_expands_take_profit_grid() -> None:
    bullish, bearish, neutral = _build_strategy_sets("SPY", (5, 10, 15))

    assert len(bullish) == 6
    assert len(bearish) == 18
    assert len(neutral) == 6
    assert {strategy.profit_target_pct for strategy in bullish} == {5, 10, 15}
    assert {strategy.profit_target_pct for strategy in bearish} == {5, 10, 15}
    assert {strategy.profit_target_pct for strategy in neutral} == {5, 10, 15}
    assert bullish[0].label == "spy_call_d40_pt5"
    assert bearish[-1].label == "bear_spy_put_d50_pt15"
    assert neutral[-1].label == "neutral_spy_call_d50_pt15"
