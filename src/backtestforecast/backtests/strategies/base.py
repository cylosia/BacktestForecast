from __future__ import annotations

from typing import Protocol

from backtestforecast.backtests.types import BacktestConfig, OpenMultiLegPosition, OptionDataGateway
from backtestforecast.market_data.types import DailyBar


class StrategyDefinition(Protocol):
    strategy_type: str
    margin_warning_message: str | None

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
    ) -> OpenMultiLegPosition | None: ...
