from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from backtestforecast.backtests.strategies.long_options import LONG_CALL_STRATEGY, LONG_PUT_STRATEGY
from backtestforecast.market_data.service import HistoricalDataBundle, MarketDataService
from backtestforecast.market_data.types import DailyBar
from backtestforecast.schemas.backtests import CreateBacktestRunRequest, StrategyType


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
) -> ContractCatalogPrewarmSummary:
    contract_type = resolve_long_option_contract_type(request.strategy_type)
    bundle = market_data_service.prepare_backtest(request)
    return prewarm_long_option_bundle(
        request,
        bundle=bundle,
        include_quotes=include_quotes,
        max_dates=max_dates,
    )


def prewarm_long_option_bundle(
    request: CreateBacktestRunRequest,
    *,
    bundle: HistoricalDataBundle,
    include_quotes: bool = False,
    max_dates: int | None = None,
) -> ContractCatalogPrewarmSummary:
    contract_type = resolve_long_option_contract_type(request.strategy_type)
    gateway = bundle.option_gateway
    exact_fetch = getattr(gateway, "list_contracts_for_preferred_expiration", None)
    if not callable(exact_fetch):
        raise TypeError("option gateway must expose list_contracts_for_preferred_expiration for exact prewarm")

    strike_override = _resolve_long_option_strike_override(request)
    strike_band_resolver = LONG_CALL_STRATEGY if contract_type == "call" else LONG_PUT_STRATEGY
    summary = ContractCatalogPrewarmSummary(
        symbol=request.symbol,
        strategy_type=request.strategy_type.value,
    )
    for bar in collect_trade_dates(
        bundle.bars,
        start_date=request.start_date,
        end_date=request.end_date,
        max_dates=max_dates,
    ):
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
                for contract in contracts:
                    gateway.get_quote(contract.ticker, bar.trade_date)
                    summary.quotes_fetched += 1
        except Exception as exc:
            summary.errors.append(f"{request.symbol} {bar.trade_date}: {exc}")
    return summary
