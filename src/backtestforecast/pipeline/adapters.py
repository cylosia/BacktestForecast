"""Adapters that bridge the nightly pipeline to existing services.

The pipeline service is dependency-injected with abstract interfaces.
These adapters implement those interfaces using the existing
MarketDataService, BacktestExecutionService, and HistoricalAnalogForecaster.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal
from typing import Any, Self

import structlog

from backtestforecast.errors import DataUnavailableError, ExternalServiceError, ValidationError
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.service import HistoricalDataBundle
from backtestforecast.market_data.types import DailyBar
from backtestforecast.schemas.backtests import CreateBacktestRunRequest
from backtestforecast.services.backtest_execution import BacktestExecutionService

logger = structlog.get_logger("pipeline.adapters")


class PipelineMarketDataFetcher:
    """Fetches daily bars and earnings dates for the pipeline."""

    def __init__(self, client: MassiveClient) -> None:
        self.client = client
        self._earnings_cache: dict[tuple[str, date, date], set[date]] = {}
        self._earnings_cache_lock = threading.Lock()

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> list[DailyBar]:
        from backtestforecast.market_data.service import MarketDataService

        raw_bars = self.client.get_stock_daily_bars(symbol, start_date, end_date)
        return MarketDataService._validate_bars(raw_bars, symbol)

    def get_earnings_dates(self, symbol: str, start_date: date, end_date: date) -> set[date]:
        cache_key = (symbol, start_date, end_date)
        with self._earnings_cache_lock:
            cached = self._earnings_cache.get(cache_key)
            if cached is not None:
                return cached

        try:
            dates = self.client.list_earnings_event_dates(symbol, start_date, end_date)
        except ExternalServiceError:
            logger.warning("pipeline.earnings_fetch_failed", symbol=symbol)
            dates = set()

        with self._earnings_cache_lock:
            self._earnings_cache.setdefault(cache_key, dates)
            return self._earnings_cache[cache_key]


class PipelineBacktestExecutor:
    """Runs backtests for the pipeline.

    Provides two methods:
      - run_quick_backtest: 90-day lookback, returns summary dict only
      - run_full_backtest: 365-day lookback, returns full results
    """

    _MAX_BUNDLE_CACHE_SIZE = 200

    def __init__(self, execution_service: BacktestExecutionService | None = None) -> None:
        self._owns_service = execution_service is None
        self._execution_service = execution_service or BacktestExecutionService()
        self._bundle_cache: OrderedDict[tuple[str, date, date], HistoricalDataBundle] = OrderedDict()
        self._bundle_cache_lock = threading.Lock()

    def close(self) -> None:
        if self._owns_service:
            self._execution_service.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _get_bundle(self, request: CreateBacktestRunRequest) -> HistoricalDataBundle:
        key = (request.symbol, request.start_date, request.end_date)
        with self._bundle_cache_lock:
            bundle = self._bundle_cache.get(key)
            if bundle is not None:
                self._bundle_cache.move_to_end(key)
                return bundle
        bundle = self._execution_service.market_data_service.prepare_backtest(request)
        with self._bundle_cache_lock:
            if key not in self._bundle_cache:
                if len(self._bundle_cache) >= self._MAX_BUNDLE_CACHE_SIZE:
                    self._bundle_cache.popitem(last=False)
                self._bundle_cache[key] = bundle
            self._bundle_cache.move_to_end(key)
            return self._bundle_cache[key]

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
            bundle = self._get_bundle(request)
            result = self._execution_service.execute_request(request, bundle=bundle)
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
            bundle = self._get_bundle(request)
            result = self._execution_service.execute_request(request, bundle=bundle)
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
                "equity_curve": self._downsample_equity_curve(result.equity_curve),
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
            "max_holding_days": target_dte,
            "account_size": Decimal("10000"),
            "risk_per_trade_pct": Decimal("5"),
            "commission_per_contract": Decimal("0.65"),
            "entry_rules": [],
        }
        if strategy_overrides:
            payload["strategy_overrides"] = strategy_overrides
        return CreateBacktestRunRequest(**payload)

    @staticmethod
    def _downsample_equity_curve(curve: list[Any]) -> list[dict[str, Any]]:
        if not curve:
            return []
        max_dd_idx = max(range(len(curve)), key=lambda i: curve[i].drawdown_pct)
        return [
            {
                "trade_date": p.trade_date.isoformat(),
                "equity": p.equity,
                "drawdown_pct": p.drawdown_pct,
            }
            for i, p in enumerate(curve)
            if i % 5 == 0 or i == max_dd_idx or i == len(curve) - 1
        ]


class PipelineForecaster:
    """Wraps the existing HistoricalAnalogForecaster for pipeline use."""

    _MAX_BAR_CACHE_SIZE = 500

    def __init__(self, forecaster: Any, market_data: PipelineMarketDataFetcher) -> None:
        self._forecaster = forecaster
        self._market_data = market_data
        self._bar_cache: OrderedDict[tuple[str, date], list[DailyBar]] = OrderedDict()
        self._bar_cache_lock = threading.Lock()

    def get_forecast(
        self,
        symbol: str,
        strategy_type: str,
        horizon_days: int,
        *,
        as_of_date: date | None = None,
    ) -> dict[str, Any] | None:
        try:
            end_date = as_of_date or datetime.now(ZoneInfo("America/New_York")).date()
            cache_key = (symbol, end_date)
            with self._bar_cache_lock:
                bars = self._bar_cache.get(cache_key)
                if bars is not None:
                    self._bar_cache.move_to_end(cache_key)
            if bars is None:
                bars = self._market_data.get_daily_bars(
                    symbol,
                    end_date - timedelta(days=400),
                    end_date,
                )
                with self._bar_cache_lock:
                    if cache_key not in self._bar_cache:
                        if len(self._bar_cache) >= self._MAX_BAR_CACHE_SIZE:
                            self._bar_cache.popitem(last=False)
                        self._bar_cache[cache_key] = bars
                    self._bar_cache.move_to_end(cache_key)
                    bars = self._bar_cache[cache_key]
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
