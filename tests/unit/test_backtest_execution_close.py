from __future__ import annotations

from unittest.mock import MagicMock


def test_close_releases_owned_market_data_resources(monkeypatch) -> None:
    from backtestforecast.services import backtest_execution as module

    fake_market_data_service = MagicMock()
    fake_market_data_service.client = MagicMock()

    monkeypatch.setattr(module, "MassiveClient", lambda: object())
    monkeypatch.setattr(module, "MarketDataService", lambda client: fake_market_data_service)

    service = module.BacktestExecutionService()
    service.close()

    fake_market_data_service.close.assert_called_once_with()
    fake_market_data_service.client.close.assert_called_once_with()


def test_close_is_safe_across_repeated_lifecycle_calls(monkeypatch) -> None:
    from backtestforecast.services import backtest_execution as module

    fake_market_data_service = MagicMock()
    fake_market_data_service.client = MagicMock()

    monkeypatch.setattr(module, "MassiveClient", lambda: object())
    monkeypatch.setattr(module, "MarketDataService", lambda client: fake_market_data_service)

    service = module.BacktestExecutionService()
    service.close()
    service.close()

    assert fake_market_data_service.close.call_count == 2
    assert fake_market_data_service.client.close.call_count == 2
