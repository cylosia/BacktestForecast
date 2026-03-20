from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Protocol, Sequence

from backtestforecast.market_data.types import OptionContractRecord, OptionQuoteRecord
from backtestforecast.schemas.backtests import CustomLegDefinition, EntryRule, StrategyOverrides

DEFAULT_CONTRACT_MULTIPLIER: float = 100.0

_HISTORICAL_RISK_FREE_RATES: dict[int, float] = {
    2005: 0.031, 2006: 0.048, 2007: 0.045, 2008: 0.014,
    2009: 0.002, 2010: 0.001, 2011: 0.001, 2012: 0.001,
    2013: 0.001, 2014: 0.001, 2015: 0.002, 2016: 0.003,
    2017: 0.010, 2018: 0.020, 2019: 0.021, 2020: 0.004,
    2021: 0.001, 2022: 0.020, 2023: 0.052, 2024: 0.053,
    2025: 0.045, 2026: 0.045,
}


def estimate_risk_free_rate(start_date: date, end_date: date) -> float:
    """Estimate annualized risk-free rate for a backtest period.

    Uses average 3-month T-bill yields by year. Falls back to 4.5% for
    years outside the lookup table.
    """
    if start_date.year == end_date.year:
        return _HISTORICAL_RISK_FREE_RATES.get(start_date.year, 0.045)
    rates = []
    for year in range(start_date.year, end_date.year + 1):
        rates.append(_HISTORICAL_RISK_FREE_RATES.get(year, 0.045))
    return sum(rates) / len(rates) if rates else 0.045


@dataclass(frozen=True, slots=True)
class DividendRecord:
    ex_date: date
    cash_amount: float


class OptionDataGateway(Protocol):
    """Gateway for option chain and quote data for a single underlying.

    Current implementation assumes a single underlying symbol per backtest.
    For multi-underlying strategies (e.g., pairs trading, inter-market
    spreads), use ``MultiUnderlyingGateway`` which wraps per-symbol gateways.
    """

    def list_contracts(
        self,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> Sequence[OptionContractRecord]: ...

    def get_quote(self, option_ticker: str, trade_date: date) -> OptionQuoteRecord | None: ...

    def get_dividends(self, symbol: str, start_date: date, end_date: date) -> Sequence[DividendRecord]: ...


class MultiUnderlyingGateway:  # noqa: vulture — planned extension point
    """Gateway wrapper for strategies that trade options on multiple underlyings.

    **STATUS: Not yet used by any strategy.** This is the extension point for
    future multi-underlying support (e.g., pairs trading, correlation strategies).
    Retained intentionally; remove only if multi-underlying strategies are
    permanently out of scope.

    Usage::

        gateways = {
            "AAPL": aapl_gateway,
            "MSFT": msft_gateway,
        }
        multi = MultiUnderlyingGateway(gateways)
        contracts = multi.list_contracts("AAPL", entry_date, "call", 30, 5)
    """

    def __init__(self, gateways: dict[str, OptionDataGateway]) -> None:
        self._gateways = gateways

    @property
    def symbols(self) -> list[str]:
        return list(self._gateways.keys())

    def get_gateway(self, symbol: str) -> OptionDataGateway:
        gw = self._gateways.get(symbol)
        if gw is None:
            raise KeyError(f"No gateway registered for symbol: {symbol}")
        return gw

    def list_contracts(
        self,
        symbol: str,
        entry_date: date,
        contract_type: str,
        target_dte: int,
        dte_tolerance_days: int,
    ) -> Sequence[OptionContractRecord]:
        return self.get_gateway(symbol).list_contracts(
            entry_date, contract_type, target_dte, dte_tolerance_days,
        )

    def get_quote(
        self, symbol: str, option_ticker: str, trade_date: date,
    ) -> OptionQuoteRecord | None:
        return self.get_gateway(symbol).get_quote(option_ticker, trade_date)

    def get_dividends(
        self, symbol: str, start_date: date, end_date: date,
    ) -> Sequence[DividendRecord]:
        return self.get_gateway(symbol).get_dividends(symbol, start_date, end_date)


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
    risk_free_rate: float = 0.045
    dividend_yield: float = 0.0
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
    holding_period_days: int  # Calendar days between entry_date and exit_date
    entry_underlying_close: Decimal
    exit_underlying_close: Decimal
    entry_mid: Decimal  # Per-unit value / 100 (e.g., $2.50 mid → 0.025). NOT the raw option mid-price.
    exit_mid: Decimal  # Per-unit value / 100 at exit. Same convention as entry_mid.
    gross_pnl: Decimal
    net_pnl: Decimal
    total_commissions: Decimal
    entry_reason: str
    exit_reason: str
    detail_json: dict[str, Any] = field(default_factory=dict)
    holding_period_trading_days: int | None = None  # Trading days (bars) held; None for wheel strategy


@dataclass(frozen=True, slots=True)
class EquityPointResult:
    trade_date: date
    equity: Decimal
    cash: Decimal
    position_value: Decimal
    drawdown_pct: Decimal


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
    total_dividends_received: float = 0.0
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
    entry_commission_total: Decimal = Decimal("0")
    detail_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    position_value: Decimal
    position_missing_quote: bool
    missing_quote_tickers: tuple[str, ...]
