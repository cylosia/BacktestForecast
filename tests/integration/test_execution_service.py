"""Integration tests for BacktestExecutionService wiring.

These tests verify that the service correctly orchestrates
MarketDataService -> OptionsBacktestEngine -> results without
using FakeExecutionService.

NOTE: These tests require DATABASE_URL and may require MASSIVE_API_KEY
to be set. They are skipped in environments without these variables.
"""
from __future__ import annotations

import os
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def mock_massive_client():
    """Create a mock MassiveClient that returns realistic bar data."""
    from datetime import date, timedelta
    from backtestforecast.market_data.types import DailyBar

    client = MagicMock()
    start = date(2024, 1, 2)
    bars = [
        DailyBar(
            trade_date=start + timedelta(days=i),
            open_price=100.0 + i,
            high_price=102.0 + i,
            low_price=99.0 + i,
            close_price=101.0 + i,
            volume=1000000,
        )
        for i in range(252)
    ]
    client.get_stock_daily_bars.return_value = bars
    return client


def test_execution_service_produces_valid_summary(mock_massive_client):
    """Verify the wiring from request schema to engine to summary."""
    pytest.importorskip("sqlalchemy")
    if not os.environ.get("DATABASE_URL"):
        pytest.skip("DATABASE_URL required")

    from backtestforecast.services.backtest_execution import BacktestExecutionService
    from backtestforecast.market_data.service import MarketDataService
    from backtestforecast.schemas.backtests import CreateBacktestRunRequest

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
    )

    # This tests the WIRING — that the service correctly transforms
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
