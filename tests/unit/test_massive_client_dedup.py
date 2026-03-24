"""Test that MassiveClient retry logic is not duplicated.

Both _get_json and get_market_holidays should delegate to the shared
_request_with_retry method instead of duplicating the circuit-breaker
+ retry loop.
"""
from __future__ import annotations

import inspect


def test_sync_get_market_holidays_uses_request_with_retry() -> None:
    from backtestforecast.integrations.massive_client import MassiveClient
    source = inspect.getsource(MassiveClient.get_market_holidays)
    assert "_request_with_retry" in source, (
        "MassiveClient.get_market_holidays must delegate to _request_with_retry "
        "instead of duplicating the retry/circuit-breaker loop"
    )
    assert "for attempt" not in source, (
        "get_market_holidays must not contain its own retry loop"
    )


def test_async_get_market_holidays_uses_request_with_retry() -> None:
    from backtestforecast.integrations.massive_client import AsyncMassiveClient
    source = inspect.getsource(AsyncMassiveClient.get_market_holidays)
    assert "_request_with_retry" in source, (
        "AsyncMassiveClient.get_market_holidays must delegate to _request_with_retry"
    )
    assert "for attempt" not in source, (
        "async get_market_holidays must not contain its own retry loop"
    )


def test_sync_get_json_uses_request_with_retry() -> None:
    from backtestforecast.integrations.massive_client import MassiveClient
    source = inspect.getsource(MassiveClient._get_json)
    assert "_request_with_retry" in source, (
        "MassiveClient._get_json must delegate to _request_with_retry"
    )
    assert len(source.splitlines()) < 10, (
        "_get_json should be a thin wrapper (~5 lines), not a full retry loop"
    )


def test_async_get_json_uses_request_with_retry() -> None:
    from backtestforecast.integrations.massive_client import AsyncMassiveClient
    source = inspect.getsource(AsyncMassiveClient._get_json)
    assert "_request_with_retry" in source, (
        "AsyncMassiveClient._get_json must delegate to _request_with_retry"
    )
    assert len(source.splitlines()) < 10


def test_request_with_retry_has_not_found_returns_none() -> None:
    from backtestforecast.integrations.massive_client import MassiveClient
    sig = inspect.signature(MassiveClient._request_with_retry)
    assert "not_found_returns_none" in sig.parameters, (
        "_request_with_retry must accept not_found_returns_none for get_market_holidays"
    )
