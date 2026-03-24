"""Shared fake/stub classes for integration tests.

Extracted from test_api_critical_flows.py to avoid circular imports
between conftest.py and test files.
"""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

from backtestforecast.backtests.types import (
    BacktestExecutionResult,
    BacktestSummary,
    EquityPointResult,
    TradeResult,
)
from backtestforecast.market_data.types import DailyBar
from backtestforecast.schemas.scans import HistoricalAnalogForecastResponse


class FakeMarketDataService:
    def prepare_backtest(self, request):
        bars = [
            DailyBar(
                trade_date=request.start_date + timedelta(days=offset),
                open_price=100 + offset,
                high_price=101 + offset,
                low_price=99 + offset,
                close_price=100.5 + offset,
                volume=1_000_000 + (offset * 1000),
            )
            for offset in range(5)
        ]
        return SimpleNamespace(bars=bars, earnings_dates=set(), option_gateway=None)


class FakeExecutionService:
    def __init__(self) -> None:
        self.market_data_service = FakeMarketDataService()

    def close(self) -> None:
        pass

    def execute_request(self, request, bundle=None) -> BacktestExecutionResult:
        roi_lookup = {"AAPL": Decimal("12.5"), "MSFT": Decimal("6.5"), "NVDA": Decimal("15.0")}
        roi = roi_lookup.get(request.symbol, Decimal("5.0"))
        net_pnl = (Decimal(request.account_size) * roi / Decimal("100")).quantize(Decimal("0.01"))
        entry_date = request.start_date + timedelta(days=5)
        exit_date = entry_date + timedelta(days=min(request.max_holding_days, 7))
        expiration_date = exit_date + timedelta(days=max(request.target_dte - 7, 7))
        trade = TradeResult(
            option_ticker=f"{request.symbol}240119C00100000",
            strategy_type=request.strategy_type.value
            if hasattr(request.strategy_type, "value")
            else request.strategy_type,
            underlying_symbol=request.symbol,
            entry_date=entry_date,
            exit_date=exit_date,
            expiration_date=expiration_date,
            quantity=1,
            dte_at_open=request.target_dte,
            holding_period_days=(exit_date - entry_date).days,
            entry_underlying_close=100.0,
            exit_underlying_close=104.0,
            entry_mid=2.0,
            exit_mid=3.25,
            gross_pnl=float(net_pnl + Decimal(request.commission_per_contract)),
            net_pnl=float(net_pnl),
            total_commissions=float(request.commission_per_contract),
            entry_reason="=SUM(1,1)",
            exit_reason="@profit-target",
            detail_json={"scenario": "integration-test"},
        )
        summary = BacktestSummary(
            trade_count=1,
            win_rate=100.0 if roi >= 0 else 0.0,
            total_roi_pct=float(roi),
            average_win_amount=float(net_pnl),
            average_loss_amount=0.0,
            average_holding_period_days=float((exit_date - entry_date).days),
            average_dte_at_open=float(request.target_dte),
            max_drawdown_pct=2.5,
            total_commissions=float(request.commission_per_contract),
            total_net_pnl=float(net_pnl),
            starting_equity=float(request.account_size),
            ending_equity=float(Decimal(request.account_size) + net_pnl),
        )
        equity_curve = [
            EquityPointResult(
                trade_date=entry_date,
                equity=float(request.account_size),
                cash=float(request.account_size) - 200.0,
                position_value=200.0,
                drawdown_pct=0.0,
            ),
            EquityPointResult(
                trade_date=exit_date,
                equity=float(Decimal(request.account_size) + net_pnl),
                cash=float(Decimal(request.account_size) + net_pnl),
                position_value=0.0,
                drawdown_pct=0.0,
            ),
        ]
        return BacktestExecutionResult(summary=summary, trades=[trade], equity_curve=equity_curve, warnings=[])


class FakeForecaster:
    def forecast(self, *, symbol, bars, horizon_days, strategy_type=None):
        return HistoricalAnalogForecastResponse(
            symbol=symbol,
            strategy_type=strategy_type,
            as_of_date=bars[-1].trade_date,
            horizon_days=horizon_days,
            analog_count=12,
            expected_return_low_pct=Decimal("-3.0"),
            expected_return_median_pct=Decimal("4.5"),
            expected_return_high_pct=Decimal("9.0"),
            positive_outcome_rate_pct=Decimal("62.0"),
            summary="Bounded range.",
            disclaimer="Not advice.",
            analog_dates=[bars[-1].trade_date - timedelta(days=30)],
        )
