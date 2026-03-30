from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from backtestforecast.backtests.strategies.calendar import resolve_calendar_contract_groups
from backtestforecast.backtests.strategies.long_options import LONG_CALL_STRATEGY, LONG_PUT_STRATEGY
from backtestforecast.backtests.strategies.common import preferred_expiration_dates
from backtestforecast.market_data.service import HistoricalDataBundle, MarketDataService
from backtestforecast.market_data.types import DailyBar
from backtestforecast.schemas.backtests import CreateBacktestRunRequest, StrategyType

_TARGETED_SINGLE_TYPE_STRATEGIES: dict[StrategyType, str] = {
    StrategyType.COVERED_CALL: "call",
    StrategyType.CASH_SECURED_PUT: "put",
    StrategyType.BULL_CALL_DEBIT_SPREAD: "call",
    StrategyType.BEAR_PUT_DEBIT_SPREAD: "put",
    StrategyType.BULL_PUT_CREDIT_SPREAD: "put",
    StrategyType.BEAR_CALL_CREDIT_SPREAD: "call",
    StrategyType.BUTTERFLY: "call",
    StrategyType.RATIO_CALL_BACKSPREAD: "call",
    StrategyType.RATIO_PUT_BACKSPREAD: "put",
    StrategyType.NAKED_CALL: "call",
    StrategyType.NAKED_PUT: "put",
}

_TARGETED_SHARED_EXPIRATION_STRATEGIES: set[StrategyType] = {
    StrategyType.IRON_CONDOR,
    StrategyType.LONG_STRADDLE,
    StrategyType.LONG_STRANGLE,
    StrategyType.SHORT_STRADDLE,
    StrategyType.SHORT_STRANGLE,
    StrategyType.COLLAR,
    StrategyType.COVERED_STRANGLE,
    StrategyType.JADE_LIZARD,
    StrategyType.IRON_BUTTERFLY,
}

_TARGETED_CALENDAR_STRATEGIES: set[StrategyType] = {
    StrategyType.CALENDAR_SPREAD,
}


@dataclass(slots=True)
class ContractCatalogPrewarmSummary:
    symbol: str
    strategy_type: str
    dates_processed: int = 0
    contracts_fetched: int = 0
    quotes_fetched: int = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "strategy_type": self.strategy_type,
            "dates_processed": self.dates_processed,
            "contracts_fetched": self.contracts_fetched,
            "quotes_fetched": self.quotes_fetched,
            "errors": self.errors[:20],
        }


def collect_trade_dates(
    bars: list[DailyBar],
    *,
    start_date: date,
    end_date: date,
    max_dates: int | None = None,
) -> list[DailyBar]:
    trade_bars = [bar for bar in bars if start_date <= bar.trade_date <= end_date]
    if max_dates is not None:
        trade_bars = trade_bars[:max_dates]
    return trade_bars


def resolve_long_option_contract_type(strategy_type: StrategyType) -> str:
    if strategy_type == StrategyType.LONG_CALL:
        return "call"
    if strategy_type == StrategyType.LONG_PUT:
        return "put"
    raise ValueError(f"prewarm currently supports only long_call/long_put, got {strategy_type.value}")


def _resolve_long_option_strike_override(request: CreateBacktestRunRequest) -> object | None:
    overrides = request.strategy_overrides
    if overrides is None:
        return None
    if request.strategy_type == StrategyType.LONG_CALL:
        return overrides.long_call_strike
    if request.strategy_type == StrategyType.LONG_PUT:
        return overrides.long_put_strike
    return None


def prewarm_long_option_backtest(
    request: CreateBacktestRunRequest,
    *,
    market_data_service: MarketDataService,
    include_quotes: bool = False,
    max_dates: int | None = None,
    warm_future_quotes: bool = False,
) -> ContractCatalogPrewarmSummary:
    bundle = market_data_service.prepare_backtest(request)
    return prewarm_long_option_bundle(
        request,
        bundle=bundle,
        include_quotes=include_quotes,
        max_dates=max_dates,
        warm_future_quotes=warm_future_quotes,
    )


def prewarm_long_option_bundle(
    request: CreateBacktestRunRequest,
    *,
    bundle: HistoricalDataBundle,
    include_quotes: bool = False,
    max_dates: int | None = None,
    warm_future_quotes: bool = False,
) -> ContractCatalogPrewarmSummary:
    contract_type = resolve_long_option_contract_type(request.strategy_type)
    gateway = bundle.option_gateway
    exact_fetch = getattr(gateway, "list_contracts_for_preferred_expiration", None)
    if not callable(exact_fetch):
        raise TypeError("option gateway must expose list_contracts_for_preferred_expiration for exact prewarm")

    strike_override = _resolve_long_option_strike_override(request)
    strike_band_resolver = LONG_CALL_STRATEGY if contract_type == "call" else LONG_PUT_STRATEGY
    trade_bars = collect_trade_dates(
        bundle.bars,
        start_date=request.start_date,
        end_date=request.end_date,
        max_dates=max_dates,
    )
    summary = ContractCatalogPrewarmSummary(
        symbol=request.symbol,
        strategy_type=request.strategy_type.value,
    )
    for index, bar in enumerate(trade_bars):
        try:
            strike_band = strike_band_resolver._preferred_strike_band(bar.close_price, strike_override)
            contracts = exact_fetch(
                entry_date=bar.trade_date,
                contract_type=contract_type,
                target_dte=request.target_dte,
                dte_tolerance_days=request.dte_tolerance_days,
                strike_price_gte=strike_band[0] if strike_band is not None else None,
                strike_price_lte=strike_band[1] if strike_band is not None else None,
            )
            summary.dates_processed += 1
            summary.contracts_fetched += len(contracts)
            if include_quotes:
                quote_dates = (
                    _quote_trade_dates_for_entry(
                        trade_bars=trade_bars,
                        entry_index=index,
                        max_holding_days=request.max_holding_days,
                    )
                    if warm_future_quotes
                    else [bar.trade_date]
                )
                for contract in contracts:
                    for quote_date in quote_dates:
                        if quote_date > contract.expiration_date:
                            break
                        gateway.get_quote(contract.ticker, quote_date)
                        summary.quotes_fetched += 1
        except Exception as exc:
            summary.errors.append(f"{request.symbol} {bar.trade_date}: {exc}")
    return summary


def supports_targeted_exact_quote_prewarm(strategy_type: StrategyType) -> bool:
    return (
        strategy_type in _TARGETED_SINGLE_TYPE_STRATEGIES
        or strategy_type in _TARGETED_SHARED_EXPIRATION_STRATEGIES
        or strategy_type in _TARGETED_CALENDAR_STRATEGIES
    )


def prewarm_targeted_option_bundle(
    request: CreateBacktestRunRequest,
    *,
    bundle: HistoricalDataBundle,
    include_quotes: bool = False,
    max_dates: int | None = None,
    warm_future_quotes: bool = False,
) -> ContractCatalogPrewarmSummary:
    gateway = bundle.option_gateway
    exact_fetch = getattr(gateway, "list_contracts_for_preferred_expiration", None)
    exact_by_expiration = getattr(gateway, "list_contracts_for_expiration", None)
    if not callable(exact_fetch) or not callable(exact_by_expiration):
        raise TypeError("option gateway must expose exact-expiration fetch helpers for targeted prewarm")

    trade_bars = collect_trade_dates(
        bundle.bars,
        start_date=request.start_date,
        end_date=request.end_date,
        max_dates=max_dates,
    )
    summary = ContractCatalogPrewarmSummary(
        symbol=request.symbol,
        strategy_type=request.strategy_type.value,
    )
    for index, bar in enumerate(trade_bars):
        try:
            strike_band = _default_targeted_strike_band(bar.close_price)
            contract_groups = _targeted_contract_groups_for_date(
                request,
                gateway=gateway,
                trade_date=bar.trade_date,
                strike_band=strike_band,
            )
            summary.dates_processed += 1
            summary.contracts_fetched += sum(len(group) for group in contract_groups)
            if include_quotes:
                quote_dates = (
                    _quote_trade_dates_for_entry(
                        trade_bars=trade_bars,
                        entry_index=index,
                        max_holding_days=request.max_holding_days,
                        max_quote_dates=2,
                    )
                    if warm_future_quotes
                    else [bar.trade_date]
                )
                for contracts in contract_groups:
                    for contract in contracts:
                        for quote_date in quote_dates:
                            if quote_date > contract.expiration_date:
                                break
                            gateway.get_quote(contract.ticker, quote_date)
                            summary.quotes_fetched += 1
        except Exception as exc:
            summary.errors.append(f"{request.symbol} {bar.trade_date}: {exc}")
    return summary


def _quote_trade_dates_for_entry(
    *,
    trade_bars: list[DailyBar],
    entry_index: int,
    max_holding_days: int,
    max_quote_dates: int | None = None,
) -> list[date]:
    entry_date = trade_bars[entry_index].trade_date
    quote_dates: list[date] = []
    for bar in trade_bars[entry_index:]:
        if (bar.trade_date - entry_date).days > max_holding_days:
            break
        quote_dates.append(bar.trade_date)
        if max_quote_dates is not None and len(quote_dates) >= max_quote_dates:
            break
    return quote_dates


def _default_targeted_strike_band(underlying_close: float) -> tuple[float, float]:
    buffer = max(30.0, underlying_close * 0.20)
    return (max(0.5, underlying_close - buffer), underlying_close + buffer)


def _targeted_contract_groups_for_date(
    request: CreateBacktestRunRequest,
    *,
    gateway: object,
    trade_date: date,
    strike_band: tuple[float, float],
) -> list[list[object]]:
    if request.strategy_type in _TARGETED_SINGLE_TYPE_STRATEGIES:
        contract_type = _TARGETED_SINGLE_TYPE_STRATEGIES[request.strategy_type]
        contracts = gateway.list_contracts_for_preferred_expiration(
            entry_date=trade_date,
            contract_type=contract_type,
            target_dte=request.target_dte,
            dte_tolerance_days=request.dte_tolerance_days,
            strike_price_gte=strike_band[0],
            strike_price_lte=strike_band[1],
        )
        return [contracts]

    if request.strategy_type in _TARGETED_SHARED_EXPIRATION_STRATEGIES:
        for expiration_date in preferred_expiration_dates(
            trade_date,
            request.target_dte,
            request.dte_tolerance_days,
        ):
            calls = gateway.list_contracts_for_expiration(
                entry_date=trade_date,
                contract_type="call",
                expiration_date=expiration_date,
                strike_price_gte=strike_band[0],
                strike_price_lte=strike_band[1],
            )
            puts = gateway.list_contracts_for_expiration(
                entry_date=trade_date,
                contract_type="put",
                expiration_date=expiration_date,
                strike_price_gte=strike_band[0],
                strike_price_lte=strike_band[1],
            )
            if calls and puts:
                return [calls, puts]
        raise ValueError("No shared expiration was available for targeted prewarm")

    if request.strategy_type in _TARGETED_CALENDAR_STRATEGIES:
        contract_type = getattr(request.strategy_overrides, "calendar_contract_type", None) or "call"
        _, near_contracts, _, far_contracts = resolve_calendar_contract_groups(
            gateway,
            entry_date=trade_date,
            contract_type=contract_type,
            target_dte=request.target_dte,
            dte_tolerance_days=request.dte_tolerance_days,
            strike_price_gte=strike_band[0],
            strike_price_lte=strike_band[1],
        )
        return [near_contracts, far_contracts]

    raise ValueError(f"targeted prewarm is not configured for {request.strategy_type.value}")
