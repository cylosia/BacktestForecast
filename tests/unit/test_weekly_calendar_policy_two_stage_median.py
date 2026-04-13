from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from grid_search_weekly_calendar_policy_two_stage import StrategyMaskSummary, _combine_median_value


def _summary(*values: float) -> StrategyMaskSummary:
    return StrategyMaskSummary(
        trade_count=len(values),
        assignment_count=0,
        put_assignment_count=0,
        total_net_pnl=0.0,
        roi_count=len(values),
        roi_sum=sum(values),
        roi_values=tuple(sorted(values)),
        win_count=0,
        win_sum=0.0,
        loss_count=0,
        loss_sum=0.0,
    )


def _naive_combined_median(*groups: tuple[float, ...]) -> float:
    merged = sorted(value for group in groups for value in group)
    if not merged:
        return 0.0
    low = (len(merged) - 1) // 2
    high = len(merged) // 2
    return (merged[low] + merged[high]) / 2.0


def test_combine_median_value_matches_naive_merge_for_odd_count() -> None:
    first = _summary(1.0, 4.0, 8.0)
    second = _summary(2.0, 7.0)
    third = _summary(3.0, 5.0, 6.0)
    assert _combine_median_value(first, second, third) == _naive_combined_median(
        first.roi_values,
        second.roi_values,
        third.roi_values,
    )


def test_combine_median_value_matches_naive_merge_for_even_count_and_duplicates() -> None:
    first = _summary(-5.0, 1.0, 1.0)
    second = _summary(1.0, 4.0, 9.0)
    third = _summary(1.0, 2.0)
    assert _combine_median_value(first, second, third) == _naive_combined_median(
        first.roi_values,
        second.roi_values,
        third.roi_values,
    )


def test_combine_median_value_handles_empty_groups() -> None:
    first = _summary()
    second = _summary()
    third = _summary(10.0, 12.0, 14.0, 16.0)
    assert _combine_median_value(first, second, third) == _naive_combined_median(
        first.roi_values,
        second.roi_values,
        third.roi_values,
    )
