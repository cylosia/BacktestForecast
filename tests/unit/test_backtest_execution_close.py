from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock


def test_close_releases_owned_market_data_resources_after_lazy_init(monkeypatch) -> None:
    from backtestforecast.services import backtest_execution as module

    fake_market_data_service = MagicMock()
    fake_market_data_service.client = MagicMock()

    monkeypatch.setattr(module, "MassiveClient", lambda: object())
    monkeypatch.setattr(module, "MarketDataService", lambda client: fake_market_data_service)

    service = module.BacktestExecutionService()
    assert service.market_data_service is fake_market_data_service
    service.close()

    fake_market_data_service.close.assert_called_once_with()
    fake_market_data_service.client.close.assert_called_once_with()


def test_close_does_not_instantiate_market_data_service_when_unused(monkeypatch) -> None:
    from backtestforecast.services import backtest_execution as module

    market_data_ctor = MagicMock(side_effect=AssertionError("market_data_service should stay lazy"))

    monkeypatch.setattr(module, "MassiveClient", lambda: object())
    monkeypatch.setattr(module, "MarketDataService", market_data_ctor)

    service = module.BacktestExecutionService()
    service.close()

    market_data_ctor.assert_not_called()


def test_close_is_idempotent_across_repeated_calls(monkeypatch) -> None:
    from backtestforecast.services import backtest_execution as module

    fake_market_data_service = MagicMock()
    fake_market_data_service.client = MagicMock()

    monkeypatch.setattr(module, "MassiveClient", lambda: object())
    monkeypatch.setattr(module, "MarketDataService", lambda client: fake_market_data_service)

    service = module.BacktestExecutionService()
    _ = service.market_data_service
    service.close()
    service.close()

    fake_market_data_service.close.assert_called_once_with()
    fake_market_data_service.client.close.assert_called_once_with()


def test_execute_request_can_stay_provider_lazy_with_prepared_bundle(monkeypatch) -> None:
    from backtestforecast.backtests.types import BacktestExecutionResult
    from backtestforecast.services import backtest_execution as module

    market_data_ctor = MagicMock(side_effect=AssertionError("market_data_service should stay lazy"))

    monkeypatch.setattr(module, "MassiveClient", lambda: object())
    monkeypatch.setattr(module, "MarketDataService", market_data_ctor)
    monkeypatch.setattr(
        module,
        "get_settings",
        lambda: SimpleNamespace(
            option_cache_warn_age_seconds=259_200,
            backtest_option_prefetch_enabled=False,
        ),
    )

    class _Engine:
        def run(self, *, config, bars, earnings_dates, ex_dividend_dates, option_gateway):
            return BacktestExecutionResult(
                summary=SimpleNamespace(ending_equity=10100.0, starting_equity=10000.0),
                trades=[],
                equity_curve=[],
                warnings=[],
            )

    bundle = SimpleNamespace(
        bars=[
            SimpleNamespace(trade_date=date(2025, 4, 1)),
            SimpleNamespace(trade_date=date(2025, 4, 2)),
        ],
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=SimpleNamespace(),
        data_source="historical_flatfile",
        warnings=[],
    )
    request = module.CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date=date(2025, 4, 1),
        end_date=date(2025, 4, 2),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[],
        risk_free_rate=Decimal("0.02"),
    )

    service = module.BacktestExecutionService(engine=_Engine())
    service.execute_request(request, bundle=bundle)

    market_data_ctor.assert_not_called()
