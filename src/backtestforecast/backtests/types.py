from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Protocol, Sequence

from backtestforecast.market_data.types import OptionContractRecord, OptionQuoteRecord
from backtestforecast.schemas.backtests import CustomLegDefinition, EntryRule, StrategyOverrides


class OptionDataGateway(Protocol):
    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> Sequence[OptionContractRecord]: ...

    def select_contract(
        self,
        entry_date: date,
        strategy_type: str,
        underlying_close: float,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> OptionContractRecord: ...

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None: ...


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    symbol: str
    strategy_type: str
    start_date: date
    end_date: date
    target_dte: int
    dte_tolerance_days: int
    max_holding_days: int
    account_size: float
    risk_per_trade_pct: float
    commission_per_contract: float
    entry_rules: Sequence[EntryRule]
    strategy_overrides: StrategyOverrides | None = None
    custom_legs: Sequence[CustomLegDefinition] | None = None


@dataclass(frozen=True, slots=True)
class TradeResult:
    option_ticker: str
    strategy_type: str
    underlying_symbol: str
    entry_date: date
    exit_date: date
    expiration_date: date
    quantity: int
    dte_at_open: int
    holding_period_days: int
    entry_underlying_close: float
    exit_underlying_close: float
    entry_mid: float
    exit_mid: float
    gross_pnl: float
    net_pnl: float
    total_commissions: float
    entry_reason: str
    exit_reason: str
    detail_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EquityPointResult:
    trade_date: date
    equity: float
    cash: float
    position_value: float
    drawdown_pct: float


@dataclass(frozen=True, slots=True)
class BacktestSummary:
    trade_count: int
    win_rate: float
    total_roi_pct: float
    average_win_amount: float
    average_loss_amount: float
    average_holding_period_days: float
    average_dte_at_open: float
    max_drawdown_pct: float
    total_commissions: float
    total_net_pnl: float
    starting_equity: float
    ending_equity: float


@dataclass(frozen=True, slots=True)
class BacktestExecutionResult:
    summary: BacktestSummary
    trades: list[TradeResult]
    equity_curve: list[EquityPointResult]
    warnings: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class OpenOptionLeg:
    ticker: str
    contract_type: str
    side: int
    strike_price: float
    expiration_date: date
    quantity_per_unit: int
    entry_mid: float
    last_mid: float


@dataclass(slots=True)
class OpenStockLeg:
    symbol: str
    side: int
    share_quantity_per_unit: int
    entry_price: float
    last_price: float


@dataclass(slots=True)
class OpenMultiLegPosition:
    display_ticker: str
    strategy_type: str
    underlying_symbol: str
    entry_date: date
    entry_index: int
    quantity: int
    dte_at_open: int
    option_legs: list[OpenOptionLeg]
    stock_legs: list[OpenStockLeg] = field(default_factory=list)
    scheduled_exit_date: date | None = None
    capital_required_per_unit: float = 0.0
    max_loss_per_unit: float | None = None
    max_profit_per_unit: float | None = None
    entry_reason: str = "entry_rules_met"
    detail_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    position_value: float
    position_missing_quote: bool
    missing_quote_tickers: list[str]
