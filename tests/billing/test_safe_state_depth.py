"""Test that _safe_state handles deeply nested dicts without RecursionError."""
from __future__ import annotations

from backtestforecast.billing.events import _safe_state


def test_deeply_nested_dict_does_not_overflow() -> None:
    """A dict nested beyond _MAX_DEPTH should be truncated, not crash."""
    nested: dict = {}
    current = nested
    for i in range(50):
        current["level"] = {}
        current = current["level"]
    current["leaf"] = "value"

    result = _safe_state(nested)
    assert result is not None

    depth = 0
    node = result
    while isinstance(node, dict) and "level" in node:
        node = node["level"]
        depth += 1
    assert depth <= 11, f"Expected truncation at depth ~10, reached {depth}"


def test_redacts_sensitive_keys_in_nested() -> None:
    state = {"outer": {"email": "user@example.com", "plan": "pro"}}
    result = _safe_state(state)
    assert result["outer"]["email"] == "<redacted>"
    assert result["outer"]["plan"] == "pro"


def test_none_input() -> None:
    assert _safe_state(None) is None


def test_empty_input() -> None:
    assert _safe_state({}) == {}
