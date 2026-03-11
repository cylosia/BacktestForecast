"""Adapters that bridge the nightly pipeline to existing services.

The pipeline service is dependency-injected with abstract interfaces.
These adapters implement those interfaces using the existing
MarketDataService, BacktestExecutionService, and HistoricalAnalogForecaster.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Self

import structlog

from backtestforecast.errors import DataUnavailableError, ExternalServiceError, ValidationError
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.types import DailyBar
from backtestforecast.schemas.backtests import CreateBacktestRunRequest
from backtestforecast.services.backtest_execution import BacktestExecutionService

logger = structlog.get_logger("pipeline.adapters")


class PipelineMarketDataFetcher:
    """Fetches daily bars for the pipeline's universe screening stage."""

    def __init__(self, client: MassiveClient) -> None:
        self.client = client

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> list[DailyBar]:
        bars = self.client.get_stock_daily_bars(symbol, start_date, end_date)
        return sorted(bars, key=lambda b: b.trade_date)


class PipelineBacktestExecutor:
    """Runs backtests for the pipeline.

    Provides two methods:
      - run_quick_backtest: 90-day lookback, returns summary dict only
      - run_full_backtest: 365-day lookback, returns full results
    """

    def __init__(self) -> None:
        self._execution_service = BacktestExecutionService()

    def close(self) -> None:
        self._execution_service.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def run_quick_backtest(
        self,
        symbol: str,
        strategy_type: str,
        start_date: date,
        end_date: date,
        target_dte: int = 30,
        strategy_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Run a short-lookback backtest and return just the summary."""
        try:
            request = self._build_request(
                symbol=symbol,
                strategy_type=strategy_type,
                start_date=start_date,
                end_date=end_date,
                target_dte=target_dte,
                strategy_overrides=strategy_overrides,
            )
            result = self._execution_service.execute_request(request)
            return {
                "trade_count": result.summary.trade_count,
                "win_rate": result.summary.win_rate,
                "total_roi_pct": result.summary.total_roi_pct,
                "total_net_pnl": result.summary.total_net_pnl,
                "max_drawdown_pct": result.summary.max_drawdown_pct,
                "average_holding_period_days": result.summary.average_holding_period_days,
            }
        except (DataUnavailableError, ExternalServiceError, ValidationError):
            logger.warning("pipeline.quick_backtest_failed", symbol=symbol, strategy_type=strategy_type, exc_info=True)
            return None

    def run_full_backtest(
        self,
        symbol: str,
        strategy_type: str,
        start_date: date,
        end_date: date,
        target_dte: int = 30,
        strategy_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Run a full-lookback backtest with trades and equity curve."""
        try:
            request = self._build_request(
                symbol=symbol,
                strategy_type=strategy_type,
                start_date=start_date,
                end_date=end_date,
                target_dte=target_dte,
                strategy_overrides=strategy_overrides,
            )
            result = self._execution_service.execute_request(request)
            return {
                "trade_count": result.summary.trade_count,
                "win_rate": result.summary.win_rate,
                "total_roi_pct": result.summary.total_roi_pct,
                "total_net_pnl": result.summary.total_net_pnl,
                "max_drawdown_pct": result.summary.max_drawdown_pct,
                "average_holding_period_days": result.summary.average_holding_period_days,
                "starting_equity": result.summary.starting_equity,
                "ending_equity": result.summary.ending_equity,
                "trades": [
                    {
                        "entry_date": t.entry_date.isoformat(),
                        "exit_date": t.exit_date.isoformat(),
                        "net_pnl": t.net_pnl,
                        "holding_period_days": t.holding_period_days,
                    }
                    for t in result.trades[:50]  # Cap trade detail for storage
                ],
                "equity_curve": [
                    {
                        "trade_date": p.trade_date.isoformat(),
                        "equity": p.equity,
                        "drawdown_pct": p.drawdown_pct,
                    }
                    for p in result.equity_curve
                ],
                "warnings": [w for w in result.warnings],
            }
        except (DataUnavailableError, ExternalServiceError, ValidationError):
            logger.warning("pipeline.full_backtest_failed", symbol=symbol, strategy_type=strategy_type, exc_info=True)
            return None

    @staticmethod
    def _build_request(
        symbol: str,
        strategy_type: str,
        start_date: date,
        end_date: date,
        target_dte: int,
        strategy_overrides: dict[str, Any] | None,
    ) -> CreateBacktestRunRequest:
        payload: dict[str, Any] = {
            "symbol": symbol,
            "strategy_type": strategy_type,
            "start_date": start_date,
            "end_date": end_date,
            "target_dte": target_dte,
            "dte_tolerance_days": 5,
            "max_holding_days": min(target_dte, 30),
            "account_size": Decimal("10000"),
            "risk_per_trade_pct": Decimal("5"),
            "commission_per_contract": Decimal("0.65"),
            "entry_rules": [
                {"type": "rsi", "operator": "lte", "threshold": Decimal("40"), "period": 14},
            ],
        }
        if strategy_overrides:
            payload["strategy_overrides"] = strategy_overrides
        return CreateBacktestRunRequest(**payload)


class PipelineForecaster:
    """Wraps the existing HistoricalAnalogForecaster for pipeline use."""

    def __init__(self, forecaster: Any, market_data: PipelineMarketDataFetcher) -> None:
        self._forecaster = forecaster
        self._market_data = market_data

    def get_forecast(
        self,
        symbol: str,
        strategy_type: str,
        horizon_days: int,
    ) -> dict[str, Any] | None:
        try:
            end_date = date.today()
            bars = self._market_data.get_daily_bars(
                symbol,
                end_date - timedelta(days=400),
                end_date,
            )
            if len(bars) < 80:
                return None
            result = self._forecaster.forecast(
                symbol=symbol,
                bars=bars,
                horizon_days=horizon_days,
                strategy_type=strategy_type,
            )
            return {
                "expected_return_low_pct": float(result.expected_return_low_pct),
                "expected_return_median_pct": float(result.expected_return_median_pct),
                "expected_return_high_pct": float(result.expected_return_high_pct),
                "positive_outcome_rate_pct": float(result.positive_outcome_rate_pct),
                "analog_count": result.analog_count,
                "horizon_days": result.horizon_days,
            }
        except (DataUnavailableError, ExternalServiceError, ValidationError):
            logger.warning("pipeline.forecast_failed", symbol=symbol, exc_info=True)
            return None
