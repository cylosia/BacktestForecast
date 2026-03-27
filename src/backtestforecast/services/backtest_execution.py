from __future__ import annotations

import structlog

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.types import BacktestConfig, BacktestExecutionResult
from backtestforecast.config import get_settings
from backtestforecast.domain.execution_parameters import ResolvedExecutionParameters
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.prefetch import OptionDataPrefetcher
from backtestforecast.market_data.service import HistoricalDataBundle, MarketDataService
from backtestforecast.schemas.backtests import CreateBacktestRunRequest
from backtestforecast.services.risk_free_rate import (
    build_backtest_risk_free_rate_curve,
    resolve_backtest_risk_free_rate,
)

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
            self.market_data_service.close()
            self.market_data_service.client.close()

    def __enter__(self) -> BacktestExecutionService:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def execute_request(
        self,
        request: CreateBacktestRunRequest,
        bundle: HistoricalDataBundle | None = None,
        resolved_parameters: ResolvedExecutionParameters | None = None,
    ) -> BacktestExecutionResult:
        if self._closed:
            raise RuntimeError("BacktestExecutionService has been closed and cannot be reused.")
        settings = get_settings()
        resolved_bundle = bundle or self.market_data_service.prepare_backtest(request)
        self._maybe_prefetch_option_data(request, resolved_bundle, settings)
        parameters = resolved_parameters
        if parameters is None:
            resolved_risk_free_rate = resolve_backtest_risk_free_rate(
                request,
                client=self.market_data_service.client,
            )
            parameters = ResolvedExecutionParameters.from_request_resolution(
                request,
                resolved_risk_free_rate,
            )
        resolved_risk_free_rate_curve = build_backtest_risk_free_rate_curve(
            request,
            default_rate=parameters.risk_free_rate or 0.0,
            client=self.market_data_service.client,
        )
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
            risk_free_rate=parameters.risk_free_rate or 0.0,
            risk_free_rate_curve=resolved_risk_free_rate_curve,
            dividend_yield=parameters.dividend_yield,
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
        if resolved_bundle.warnings:
            result.warnings.extend(resolved_bundle.warnings)
        object.__setattr__(result, "data_source", resolved_bundle.data_source)
        self._check_data_staleness(request.symbol, result, settings)
        return result

    def _maybe_prefetch_option_data(
        self,
        request: CreateBacktestRunRequest,
        bundle: HistoricalDataBundle,
        settings: object,
    ) -> None:
        if not getattr(settings, "backtest_option_prefetch_enabled", True):
            return
        trade_dates = [
            bar.trade_date for bar in bundle.bars
            if request.start_date <= bar.trade_date <= request.end_date
        ]
        if len(trade_dates) < getattr(settings, "backtest_prefetch_min_trade_dates", 10):
            return
        try:
            summary = OptionDataPrefetcher(
                timeout_seconds=getattr(settings, "backtest_prefetch_timeout_seconds", 180),
            ).prefetch_for_symbol(
                request.symbol,
                bundle.bars,
                request.start_date,
                request.end_date,
                request.target_dte,
                request.dte_tolerance_days,
                bundle.option_gateway,
                include_quotes=False,
                max_dates=getattr(settings, "backtest_prefetch_max_dates", 6),
            )
            _logger.info(
                "backtest.option_prefetch_completed",
                symbol=request.symbol,
                summary=summary.to_dict(),
            )
        except Exception:
            _logger.warning("backtest.option_prefetch_failed", symbol=request.symbol, exc_info=True)

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
