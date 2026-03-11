from __future__ import annotations

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.types import BacktestConfig, BacktestExecutionResult
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.service import HistoricalDataBundle, MarketDataService
from backtestforecast.schemas.backtests import CreateBacktestRunRequest


class BacktestExecutionService:
    def __init__(
        self,
        market_data_service: MarketDataService | None = None,
        engine: OptionsBacktestEngine | None = None,
    ) -> None:
        self._owns_client = market_data_service is None
        self.market_data_service = market_data_service or MarketDataService(MassiveClient())
        self.engine = engine or OptionsBacktestEngine()

    def close(self) -> None:
        if self._owns_client:
            self.market_data_service.client.close()

    def execute_request(
        self,
        request: CreateBacktestRunRequest,
        bundle: HistoricalDataBundle | None = None,
    ) -> BacktestExecutionResult:
        resolved_bundle = bundle or self.market_data_service.prepare_backtest(request)
        config = BacktestConfig(
            symbol=request.symbol,
            strategy_type=request.strategy_type.value,
            start_date=request.start_date,
            end_date=request.end_date,
            target_dte=request.target_dte,
            dte_tolerance_days=request.dte_tolerance_days,
            max_holding_days=request.max_holding_days,
            account_size=float(request.account_size),
            risk_per_trade_pct=float(request.risk_per_trade_pct),
            commission_per_contract=float(request.commission_per_contract),
            entry_rules=request.entry_rules,
            strategy_overrides=request.strategy_overrides,
            custom_legs=request.custom_legs,
        )
        return self.engine.run(
            config=config,
            bars=resolved_bundle.bars,
            earnings_dates=resolved_bundle.earnings_dates,
            option_gateway=resolved_bundle.option_gateway,
        )
