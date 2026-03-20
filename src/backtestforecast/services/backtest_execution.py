from __future__ import annotations

import structlog

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.types import BacktestConfig, BacktestExecutionResult
from backtestforecast.config import get_settings
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.service import HistoricalDataBundle, MarketDataService
from backtestforecast.schemas.backtests import CreateBacktestRunRequest

_logger = structlog.get_logger("services.backtest_execution")


class BacktestExecutionService:
    """Orchestrates market data fetching and backtest engine execution.

    Thread safety: instances hold mutable state (_owns_client,
    market_data_service, engine). Do NOT share across threads without
    external synchronization. Create one instance per thread/task.
    """

    def __init__(
        self,
        market_data_service: MarketDataService | None = None,
        engine: OptionsBacktestEngine | None = None,
    ) -> None:
        self._owns_client = market_data_service is None
        self._closed = False
        self.market_data_service = market_data_service or MarketDataService(MassiveClient())
        self.engine = engine or OptionsBacktestEngine()

    def close(self) -> None:
        self._closed = True
        if self._owns_client:
            self.market_data_service.client.close()

    def __enter__(self) -> "BacktestExecutionService":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def execute_request(
        self,
        request: CreateBacktestRunRequest,
        bundle: HistoricalDataBundle | None = None,
    ) -> BacktestExecutionResult:
        if self._closed:
            raise RuntimeError("BacktestExecutionService has been closed and cannot be reused.")
        settings = get_settings()
        resolved_bundle = bundle or self.market_data_service.prepare_backtest(request)
        from backtestforecast.backtests.types import estimate_risk_free_rate

        if request.risk_free_rate is not None:
            rfr = float(request.risk_free_rate)
        else:
            rfr = settings.risk_free_rate
            if rfr == 0.045:
                rfr = estimate_risk_free_rate(request.start_date, request.end_date)
        div_yield = float(request.dividend_yield) if request.dividend_yield is not None else 0.0
        config = BacktestConfig(
            symbol=request.symbol,
            strategy_type=request.strategy_type.value,
            start_date=request.start_date,
            end_date=request.end_date,
            target_dte=request.target_dte,
            dte_tolerance_days=request.dte_tolerance_days,
            max_holding_days=request.max_holding_days,
            account_size=request.account_size,
            risk_per_trade_pct=request.risk_per_trade_pct,
            commission_per_contract=request.commission_per_contract,
            entry_rules=request.entry_rules,
            risk_free_rate=rfr,
            dividend_yield=div_yield,
            slippage_pct=request.slippage_pct,
            strategy_overrides=request.strategy_overrides,
            custom_legs=request.custom_legs,
            profit_target_pct=request.profit_target_pct,
            stop_loss_pct=request.stop_loss_pct,
        )
        result = self.engine.run(
            config=config,
            bars=resolved_bundle.bars,
            earnings_dates=resolved_bundle.earnings_dates,
            ex_dividend_dates=resolved_bundle.ex_dividend_dates,
            option_gateway=resolved_bundle.option_gateway,
        )
        self._check_data_staleness(request.symbol, result, settings)
        return result

    def _check_data_staleness(
        self,
        symbol: str,
        result: BacktestExecutionResult,
        settings: object,
    ) -> None:
        """Add a warning to backtest results if cached option data is stale."""
        try:
            cache = getattr(self.market_data_service, '_redis_cache', None)
            if cache is None:
                return
            warn_age = getattr(settings, 'option_cache_warn_age_seconds', 259_200)
            age = cache.get_oldest_cache_age_seconds(symbol)
            if age is not None and age > warn_age:
                days = int(age / 86400)
                warning = {
                    "code": "stale_option_cache",
                    "message": (
                        f"Option data for {symbol} was cached {days} day(s) ago. "
                        f"Results may not reflect the most recent market conditions."
                    ),
                }
                if result.warnings is None:
                    result.warnings = [warning]
                else:
                    result.warnings.append(warning)
                _logger.warning(
                    "backtest.stale_cache",
                    symbol=symbol,
                    cache_age_seconds=round(age),
                    warn_threshold=warn_age,
                )
        except Exception:
            _logger.warning("backtest.staleness_check_failed", symbol=symbol, exc_info=True)
