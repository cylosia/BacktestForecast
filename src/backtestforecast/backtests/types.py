from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
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
    account_size: Decimal
    risk_per_trade_pct: Decimal
    commission_per_contract: Decimal
    entry_rules: Sequence[EntryRule]
    # FIXME(#97): Use a time-varying risk-free rate based on the backtest
    # date range (e.g., average 3-month T-bill rate over the period).
    #
    # A static 4.5% rate is inaccurate for backtests spanning periods with
    # very different rate environments (e.g., 2009-2021 near-zero rates vs.
    # 2022-2024 elevated rates). This distorts Sharpe ratio, Sortino ratio,
    # and any other risk-adjusted metric that subtracts the risk-free rate.
    #
    # Recommended approach:
    # 1. Add a `RiskFreeRateProvider` protocol with a method
    #    `get_rate(trade_date: date) -> float` that looks up the 3-month
    #    T-bill rate (or Fed Funds rate) for a given date.
    # 2. Implement a concrete provider backed by a static CSV or an API
    #    (e.g., FRED DGS3MO series cached in Redis with daily refresh).
    # 3. In `build_summary`, compute the average risk-free rate across
    #    the equity curve dates rather than using a single constant.
    # 4. For per-trade Sharpe contribution, use the rate on the entry date.
    # 5. Keep the static default as a fallback when no provider is configured.
    risk_free_rate: float = 0.045
    slippage_pct: float = 0.0
    strategy_overrides: StrategyOverrides | None = None
    custom_legs: Sequence[CustomLegDefinition] | None = None
    profit_target_pct: float | None = None
    stop_loss_pct: float | None = None


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
    entry_mid: float  # Per-contract value / 100 (e.g., $2.50 mid → 0.025). NOT the raw option mid-price.
    exit_mid: float  # Per-contract value / 100 at exit. Same convention as entry_mid.
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
    decided_trades: int
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
    profit_factor: float | None = None
    payoff_ratio: float | None = None
    expectancy: float = 0.0
    sharpe_ratio: float | None = None
    sortino_ratio: float | None = None
    cagr_pct: float | None = None
    calmar_ratio: float | None = None
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    recovery_factor: float | None = None


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
    contract_multiplier: float = 100.0


@dataclass(slots=True)
class OpenStockLeg:
    symbol: str
    side: int
    share_quantity_per_unit: int
    entry_price: float
    last_price: float


@dataclass(slots=True)
class OpenMultiLegPosition:
    """Open multi-leg position returned by strategy build_position.

    Strategy implementations MUST set ``entry_index`` to the bar index (0-based)
    at which the position was opened. This is used for trade attribution and
    equity curve alignment.
    """

    display_ticker: str
    strategy_type: str
    underlying_symbol: str
    entry_date: date
    entry_index: int  # MUST be set by strategy; bar index when position opened
    quantity: int
    dte_at_open: int
    option_legs: list[OpenOptionLeg]
    stock_legs: list[OpenStockLeg] = field(default_factory=list)
    scheduled_exit_date: date | None = None
    capital_required_per_unit: float = 0.0
    max_loss_per_unit: float | None = None
    max_profit_per_unit: float | None = None
    entry_reason: str = "entry_rules_met"
    entry_commission_total: float = 0.0
    detail_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    position_value: float
    position_missing_quote: bool
    missing_quote_tickers: tuple[str, ...]
