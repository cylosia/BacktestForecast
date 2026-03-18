"""Verify global cache tracking counts all cache types, not just contracts.

Audit fixes 9-14: The original implementation only tracked contract cache
entries in _global_cache_entries, missing quote and snapshot caches.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from backtestforecast.market_data.service import (
    MassiveOptionGateway,
    _global_cache_entries,
    _global_cache_lock,
    get_global_cache_entries,
)


def _reset_global():
    """Reset the global counter to 0 for test isolation."""
    import backtestforecast.market_data.service as mod
    with _global_cache_lock:
        mod._global_cache_entries = 0


def test_contract_cache_tracked():
    _reset_global()
    client = MagicMock()
    gw = MassiveOptionGateway(client=client, symbol="AAPL")

    gw._store_contracts_in_memory(
        (date(2026, 1, 1), "call", 30, 5),
        [],
    )
    assert get_global_cache_entries() == 1


def test_quote_cache_tracked():
    _reset_global()
    client = MagicMock()
    gw = MassiveOptionGateway(client=client, symbol="AAPL")

    gw._store_quote_in_memory(("O:AAPL260101C00100000", date(2026, 1, 1)), None)
    assert get_global_cache_entries() == 1


def test_snapshot_cache_tracked_via_get_snapshot():
    """get_snapshot stores into _snapshot_cache which should be tracked."""
    _reset_global()
    client = MagicMock()
    client.get_option_snapshot.return_value = None
    gw = MassiveOptionGateway(client=client, symbol="AAPL")

    gw.get_snapshot("O:AAPL260101C00100000")
    assert get_global_cache_entries() == 1


def test_clear_caches_decrements_global():
    _reset_global()
    client = MagicMock()
    gw = MassiveOptionGateway(client=client, symbol="AAPL")

    gw._store_contracts_in_memory((date(2026, 1, 1), "call", 30, 5), [])
    gw._store_quote_in_memory(("O:AAPL260101C00100000", date(2026, 1, 1)), None)
    assert get_global_cache_entries() == 2

    gw.clear_caches()
    assert get_global_cache_entries() == 0
