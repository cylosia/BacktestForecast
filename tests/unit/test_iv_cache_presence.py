"""Verify _iv_cache is present on MassiveOptionGateway and used by strategies.

Audit fix 2-3: _iv_cache was removed during a prior refactor, causing a
silent performance regression where BSM bisection was recomputed on every
call instead of being memoized.
"""
from __future__ import annotations

from unittest.mock import MagicMock


def test_gateway_has_iv_cache():
    """MassiveOptionGateway must have _iv_cache dict on construction."""
    from backtestforecast.market_data.service import MassiveOptionGateway

    client = MagicMock()
    gw = MassiveOptionGateway(client=client, symbol="AAPL")
    assert hasattr(gw, "_iv_cache"), "_iv_cache attribute missing from MassiveOptionGateway"
    assert isinstance(gw._iv_cache, dict)


def test_clear_caches_resets_iv_cache():
    """clear_caches() must empty _iv_cache."""
    from backtestforecast.market_data.service import MassiveOptionGateway

    client = MagicMock()
    gw = MassiveOptionGateway(client=client, symbol="AAPL")
    gw._iv_cache[("O:TEST", "2026-01-01")] = 0.25
    gw.clear_caches()
    assert len(gw._iv_cache) == 0


def test_strategies_receive_iv_cache():
    """Strategy getattr calls should return the actual _iv_cache dict, not None."""
    from backtestforecast.market_data.service import MassiveOptionGateway

    client = MagicMock()
    gw = MassiveOptionGateway(client=client, symbol="AAPL")
    cache = getattr(gw, "_iv_cache", None)
    assert cache is not None, "getattr(gw, '_iv_cache', None) returned None - performance regression"
    assert isinstance(cache, dict)
