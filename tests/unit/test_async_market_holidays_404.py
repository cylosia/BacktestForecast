"""Test that AsyncMassiveClient.get_market_holidays returns [] on 404.

Before the fix, the async variant raised ExternalServiceError on 404 while
the sync variant correctly returned []. After the fix, both behave identically.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backtestforecast.errors import ExternalServiceError


def _make_mock_settings():
    mock = MagicMock()
    mock.massive_api_key = "test-key"
    mock.massive_base_url = "https://api.test.com"
    mock.massive_timeout_seconds = 5.0
    mock.massive_max_retries = 0
    mock.massive_retry_backoff_seconds = 0.1
    mock.app_env = "test"
    return mock


class TestAsyncMarketHolidays404:
    def test_sync_client_returns_empty_on_404(self):
        """Sync MassiveClient.get_market_holidays should return [] on 404."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        mock_settings = _make_mock_settings()
        with patch("backtestforecast.integrations.massive_client.get_settings", return_value=mock_settings):
            from backtestforecast.integrations.massive_client import MassiveClient
            client = MassiveClient(api_key="test-key", base_url="https://api.test.com")
            client._http = MagicMock()
            client._http.get.return_value = mock_response
            client._circuit.allow_request = MagicMock(return_value=True)

            result = client.get_market_holidays()
            assert result == []

    @pytest.mark.asyncio
    async def test_async_client_returns_empty_on_404(self):
        """Async AsyncMassiveClient.get_market_holidays should return [] on 404."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        mock_settings = _make_mock_settings()
        with patch("backtestforecast.integrations.massive_client.get_settings", return_value=mock_settings):
            from backtestforecast.integrations.massive_client import AsyncMassiveClient
            client = AsyncMassiveClient(api_key="test-key", base_url="https://api.test.com")
            await client.close()
            client._http = MagicMock()
            client._http.get = AsyncMock(return_value=mock_response)
            client._circuit.allow_request_async = AsyncMock(return_value=True)

            result = await client.get_market_holidays()
            assert result == []

    @pytest.mark.asyncio
    async def test_sync_and_async_parity_on_404(self):
        """Both sync and async must return the same value on 404."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        mock_settings = _make_mock_settings()
        with patch("backtestforecast.integrations.massive_client.get_settings", return_value=mock_settings):
            from backtestforecast.integrations.massive_client import (
                AsyncMassiveClient,
                MassiveClient,
            )

            sync_client = MassiveClient(api_key="test-key", base_url="https://api.test.com")
            sync_client._http = MagicMock()
            sync_client._http.get.return_value = mock_response
            sync_client._circuit.allow_request = MagicMock(return_value=True)

            async_client = AsyncMassiveClient(api_key="test-key", base_url="https://api.test.com")
            await async_client.close()
            async_client._http = MagicMock()
            async_client._http.get = AsyncMock(return_value=mock_response)
            async_client._circuit.allow_request_async = AsyncMock(return_value=True)

            sync_result = sync_client.get_market_holidays()
            async_result = await async_client.get_market_holidays()

            assert sync_result == async_result == []

    @pytest.mark.asyncio
    async def test_async_client_raises_on_non_404_error(self):
        """Non-404 errors should still raise ExternalServiceError."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.headers = {}

        mock_settings = _make_mock_settings()
        with patch("backtestforecast.integrations.massive_client.get_settings", return_value=mock_settings):
            from backtestforecast.integrations.massive_client import AsyncMassiveClient
            client = AsyncMassiveClient(api_key="test-key", base_url="https://api.test.com")
            await client.close()
            client._http = MagicMock()
            client._http.get = AsyncMock(return_value=mock_response)
            client._circuit.allow_request_async = AsyncMock(return_value=True)
            client._circuit.record_failure_async = AsyncMock()

            with pytest.raises(ExternalServiceError):
                await client.get_market_holidays()
