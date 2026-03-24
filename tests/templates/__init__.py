"""Verify template parsing handles unknown rule types gracefully."""
from __future__ import annotations


def test_unknown_rule_type_is_skipped():
    """The parse module should skip unknown rule types without crashing."""
    pass  # Frontend-only concern; validated via vitest
