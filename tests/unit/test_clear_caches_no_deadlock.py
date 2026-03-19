"""Test that MassiveOptionGateway.clear_caches() does not deadlock.

Before the fix, clear_caches() acquired self._lock (a threading.Lock),
then called _track_remove() which also tried to acquire self._lock.
Since threading.Lock is non-reentrant, this deadlocked.

After the fix, self._lock is a threading.RLock (reentrant), so the
nested acquisition succeeds. This test verifies the fix by calling
clear_caches() under a timeout — if it deadlocks, the test fails.
"""
from __future__ import annotations

import threading
from datetime import date
from unittest.mock import MagicMock

import pytest


def _make_gateway():
    """Create a MassiveOptionGateway with a mock client and some cached data."""
    from backtestforecast.market_data.service import MassiveOptionGateway
    from backtestforecast.market_data.types import OptionContractRecord, OptionQuoteRecord

    mock_client = MagicMock()
    gw = MassiveOptionGateway(client=mock_client, symbol="AAPL", redis_cache=None)

    gw._contract_cache[(date(2024, 1, 1), "call", 30, 5)] = [
        OptionContractRecord(
            ticker="O:AAPL240119C00150000",
            contract_type="call",
            expiration_date=date(2024, 1, 19),
            strike_price=150.0,
            shares_per_contract=100,
        )
    ]
    gw._quote_cache[("O:AAPL240119C00150000", date(2024, 1, 2))] = OptionQuoteRecord(
        trade_date=date(2024, 1, 2),
        bid_price=3.0,
        ask_price=3.20,
        participant_timestamp=None,
    )
    gw._tracked_entries = 2
    return gw


class TestClearCachesNoDeadlock:
    def test_clear_caches_completes_without_deadlock(self):
        """clear_caches() must complete within 2 seconds. A deadlock would hang forever."""
        gw = _make_gateway()
        completed = threading.Event()

        def _run():
            gw.clear_caches()
            completed.set()

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        assert completed.wait(timeout=2.0), (
            "clear_caches() did not complete within 2 seconds — likely deadlocked"
        )

    def test_clear_caches_empties_all_caches(self):
        gw = _make_gateway()
        assert len(gw._contract_cache) > 0
        assert len(gw._quote_cache) > 0

        gw.clear_caches()

        assert len(gw._contract_cache) == 0
        assert len(gw._quote_cache) == 0
        assert len(gw._snapshot_cache) == 0
        assert len(gw._iv_cache) == 0
        assert gw._chain_snapshot_loaded is False

    def test_clear_caches_updates_tracked_entries(self):
        gw = _make_gateway()
        assert gw._tracked_entries > 0

        gw.clear_caches()

        assert gw._tracked_entries == 0

    def test_concurrent_clear_caches_does_not_deadlock(self):
        """Multiple threads calling clear_caches simultaneously must not deadlock."""
        gw = _make_gateway()
        barrier = threading.Barrier(4, timeout=5)
        results = []

        def _run():
            try:
                barrier.wait()
                gw.clear_caches()
                results.append("ok")
            except Exception as exc:
                results.append(f"error: {exc}")

        threads = [threading.Thread(target=_run, daemon=True) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert len(results) == 4, f"Not all threads completed: {results}"
        assert all(r == "ok" for r in results), f"Some threads failed: {results}"

    def test_lock_is_reentrant(self):
        """The gateway must use an RLock, not a plain Lock."""
        gw = _make_gateway()
        assert isinstance(gw._lock, type(threading.RLock())), (
            f"Expected RLock, got {type(gw._lock).__name__}"
        )
