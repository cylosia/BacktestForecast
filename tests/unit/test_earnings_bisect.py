"""Test that earnings blackout rule uses bisect for O(log n) lookups.

Regression test for the performance issue where the earnings blackout
check did a linear scan of all earnings dates for every bar evaluated.
"""
from __future__ import annotations

import bisect
import inspect
from datetime import date, timedelta


def test_rules_module_uses_bisect():
    """The rules module must import bisect for O(log n) earnings lookups."""
    from backtestforecast.backtests import rules
    source = inspect.getsource(rules)
    assert "bisect" in source, "rules.py should use bisect for earnings date lookups"


def test_sorted_earnings_stored_on_init():
    """RuleEvaluator should pre-sort earnings dates on initialization."""
    from backtestforecast.backtests import rules
    source = inspect.getsource(rules.EntryRuleEvaluator)
    assert "_sorted_earnings" in source, (
        "EntryRuleEvaluator must store a sorted copy of earnings_dates as _sorted_earnings"
    )


def test_avoid_earnings_uses_bisect_left():
    """The _evaluate_avoid_earnings_rule must use bisect, not linear any()."""
    from backtestforecast.backtests import rules
    source = inspect.getsource(rules.EntryRuleEvaluator._evaluate_avoid_earnings_rule)
    assert "bisect" in source, (
        "_evaluate_avoid_earnings_rule should use bisect.bisect_left, not linear scan"
    )
    assert "any(" not in source, (
        "_evaluate_avoid_earnings_rule should not use any() linear scan"
    )


def test_bisect_logic_correctness():
    """Verify the bisect-based blackout check produces correct results."""
    sorted_earnings = [date(2024, 1, 15), date(2024, 4, 15), date(2024, 7, 15), date(2024, 10, 15)]

    def is_entry_allowed(bar_date: date, blackout_start: date, blackout_end: date) -> bool:
        lo = bisect.bisect_left(sorted_earnings, blackout_start)
        return lo >= len(sorted_earnings) or sorted_earnings[lo] > blackout_end

    assert is_entry_allowed(date(2024, 3, 1), date(2024, 2, 25), date(2024, 3, 5)) is True
    assert is_entry_allowed(date(2024, 1, 15), date(2024, 1, 10), date(2024, 1, 20)) is False
    assert is_entry_allowed(date(2024, 1, 10), date(2024, 1, 5), date(2024, 1, 14)) is True
    assert is_entry_allowed(date(2024, 1, 10), date(2024, 1, 5), date(2024, 1, 15)) is False
    assert is_entry_allowed(date(2024, 12, 1), date(2024, 11, 25), date(2024, 12, 5)) is True


def test_empty_earnings_always_allows():
    """With no earnings dates, entry should always be allowed."""
    sorted_earnings: list[date] = []
    bar_date = date(2024, 6, 15)
    blackout_start = bar_date - timedelta(days=5)
    blackout_end = bar_date + timedelta(days=5)
    lo = bisect.bisect_left(sorted_earnings, blackout_start)
    assert lo >= len(sorted_earnings)
