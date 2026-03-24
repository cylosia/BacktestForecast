"""Test that Redis SCAN patterns escape glob metacharacters in symbols.

Regression test for the bug where invalidate_symbol("*") would match
every key in the cache, or "SPY?" would match "SPYG" etc.
"""
from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

from backtestforecast.market_data.redis_cache import OptionDataRedisCache


def _make_cache() -> OptionDataRedisCache:
    """Create a cache instance with mocked Redis connection."""
    with patch.object(OptionDataRedisCache, "__init__", lambda self, *a, **kw: None):
        cache = OptionDataRedisCache.__new__(OptionDataRedisCache)
    cache._pool = MagicMock()
    cache._client = MagicMock()
    cache._lock = threading.Lock()
    cache._ttl = 604_800
    return cache


def _get_scan_pattern(cache: OptionDataRedisCache, symbol: str) -> str:
    """Call invalidate_symbol and capture the SCAN match pattern."""
    mock_redis = cache._client
    cache._conn = lambda: mock_redis  # type: ignore[attr-defined]
    mock_redis.smembers.return_value = set()
    mock_redis.scan.return_value = (0, [])
    cache.invalidate_symbol(symbol)
    call_args = mock_redis.scan.call_args
    return call_args[1].get("match") or call_args[0][1]


class TestInvalidateSymbolPatternEscape:
    def test_wildcard_symbol_is_escaped(self):
        cache = _make_cache()
        pattern = _get_scan_pattern(cache, "*")
        assert "\\*" in pattern, f"Wildcard * must be escaped, got: {pattern}"

    def test_question_mark_symbol_is_escaped(self):
        cache = _make_cache()
        pattern = _get_scan_pattern(cache, "SPY?")
        assert "\\?" in pattern, f"Question mark must be escaped, got: {pattern}"

    def test_bracket_symbol_is_escaped(self):
        cache = _make_cache()
        pattern = _get_scan_pattern(cache, "SPY[1]")
        assert "\\[" in pattern, f"Opening bracket must be escaped, got: {pattern}"
        assert "\\]" in pattern, f"Closing bracket must be escaped, got: {pattern}"

    def test_normal_symbol_not_escaped(self):
        cache = _make_cache()
        pattern = _get_scan_pattern(cache, "AAPL")
        assert "AAPL" in pattern
        stripped = pattern.replace("\\*", "").replace("\\?", "").replace("\\[", "").replace("\\]", "")
        assert "\\" not in stripped, f"Normal symbol should not be escaped, got: {pattern}"
