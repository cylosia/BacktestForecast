"""Integration tests for BacktestExecutionService wiring.

These tests verify that the service correctly orchestrates
MarketDataService -> OptionsBacktestEngine -> results without
using FakeExecutionService.

NOTE: These tests require DATABASE_URL and may require MASSIVE_API_KEY
to be set. They are skipped in environments without these variables.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def mock_massive_client():
    """Create a mock MassiveClient that returns realistic bar data."""
    from backtestforecast.market_data.types import DailyBar

    client = MagicMock()
    start = date(2023, 1, 2)
    bars = [
        DailyBar(
            trade_date=start + timedelta(days=i),
            open_price=100.0 + i,
            high_price=102.0 + i,
            low_price=99.0 + i,
            close_price=101.0 + i,
            volume=1000000,
        )
        for i in range(400)
    ]
    client.get_stock_daily_bars.return_value = bars
    client.list_ex_dividend_dates.return_value = {date(2024, 3, 15), date(2024, 5, 17)}
    client.get_average_treasury_yield.return_value = 0.02
    return client


def test_execution_service_produces_valid_summary(mock_massive_client):
    """Verify the wiring from request schema to engine to summary."""
    pytest.importorskip("sqlalchemy")
    if not os.environ.get("DATABASE_URL"):
        pytest.skip("DATABASE_URL required")

    from backtestforecast.market_data.service import MarketDataService
    from backtestforecast.schemas.backtests import CreateBacktestRunRequest
    from backtestforecast.services.backtest_execution import BacktestExecutionService

    market_data = MarketDataService(mock_massive_client)

    service = BacktestExecutionService(market_data_service=market_data)

    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date="2024-01-02",
        end_date="2024-06-28",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=21,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("0.65"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
    )

    # This tests the WIRING - that the service correctly transforms
    # the request into a BacktestConfig and passes it to the engine.
    # The actual execution may produce zero trades (no option chains)
    # but should not crash.
    from backtestforecast.errors import DataUnavailableError

    try:
        result = service.execute_request(request)
        assert result is not None
        assert hasattr(result, "summary")
        assert result.summary.starting_equity > 0
    except DataUnavailableError:
        pytest.skip("No option/chain data available (expected in CI without MASSIVE)")


class _CapturingEngine:
    def __init__(self) -> None:
        self.last_ex_dividend_dates = None
        self.last_option_gateway = None

    def run(self, *, config, bars, earnings_dates, ex_dividend_dates, option_gateway):
        from backtestforecast.backtests.summary import build_summary
        from backtestforecast.backtests.types import BacktestExecutionResult

        self.last_ex_dividend_dates = ex_dividend_dates
        self.last_option_gateway = option_gateway
        equity_curve = [
            SimpleNamespace(
                trade_date=bars[0].trade_date,
                equity=Decimal("10000"),
                cash=Decimal("10000"),
                position_value=Decimal("0"),
                drawdown_pct=Decimal("0"),
            ),
            SimpleNamespace(
                trade_date=bars[-1].trade_date,
                equity=Decimal("10100"),
                cash=Decimal("10100"),
                position_value=Decimal("0"),
                drawdown_pct=Decimal("0"),
            ),
        ]
        summary = build_summary(10000.0, 10100.0, [], equity_curve, risk_free_rate=config.risk_free_rate)
        return BacktestExecutionResult(summary=summary, trades=[], equity_curve=equity_curve, warnings=[])


def test_execution_service_passes_prepared_ex_dividend_dates_to_engine(mock_massive_client, monkeypatch):
    from backtestforecast.market_data.service import MarketDataService
    from backtestforecast.schemas.backtests import CreateBacktestRunRequest
    from backtestforecast.services.backtest_execution import BacktestExecutionService

    monkeypatch.setattr(
        "backtestforecast.services.backtest_execution.get_settings",
        lambda: SimpleNamespace(option_cache_warn_age_seconds=259_200),
    )

    market_data = MarketDataService(mock_massive_client)
    engine = _CapturingEngine()
    service = BacktestExecutionService(market_data_service=market_data, engine=engine)

    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date="2024-01-02",
        end_date="2024-06-28",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=21,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("0.65"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
        risk_free_rate=Decimal("0.0125"),
    )

    result = service.execute_request(request)

    assert result is not None
    assert engine.last_ex_dividend_dates == {date(2024, 3, 15), date(2024, 5, 17)}
    assert engine.last_option_gateway.get_ex_dividend_dates(date(2024, 1, 1), date(2024, 6, 30)) == {
        date(2024, 3, 15),
        date(2024, 5, 17),
    }
    mock_massive_client.list_ex_dividend_dates.assert_called_once()
