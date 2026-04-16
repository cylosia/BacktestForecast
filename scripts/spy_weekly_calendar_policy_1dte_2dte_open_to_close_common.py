from __future__ import annotations

import json
import math
from bisect import bisect_left
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
from backtestforecast.backtests.strategies.registry import STRATEGY_REGISTRY  # noqa: E402
from backtestforecast.backtests.types import BacktestConfig, TradeResult  # noqa: E402
from backtestforecast.errors import DataUnavailableError  # noqa: E402
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway  # noqa: E402
from backtestforecast.market_data.intraday_option_quotes import IntradayOptionQuoteCache  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.market_data.types import DailyBar  # noqa: E402
from backtestforecast.schemas.backtests import StrategyType  # noqa: E402
from spy_weekly_calendar_policy_1dte_2dte_common import (  # noqa: E402
    DEFAULT_SYMBOL,
    DEFAULT_TRAIN_START_DATE,
    DTE_TOLERANCE_DAYS,
    FAR_LEG_TARGET_DTE,
    FilterConfig,
    MAX_HOLDING_DAYS,
    REQUESTED_END_DATE,
    STARTING_EQUITY,
    StrategyConfig,
    TARGET_DTE,
    _build_bundle,
    _build_calendar_config,
    _load_risk_free_curve,
    _trade_roi_on_margin_pct,
    build_daily_entry_dates,
)


DEFAULT_BATCH_RUN_LABEL = "spy_1dte_2dte_open_to_close_day1_train2y_20240101_20251231"
DEFAULT_BATCH_SUMMARY_CSV = (
    ROOT
    / "logs"
    / "batch"
    / "weekly_calendar_policy_two_stage"
    / DEFAULT_BATCH_RUN_LABEL
    / "summary.csv"
)
CACHE_ROOT = ROOT / "logs" / "search_cache" / "weekly_calendar_policy_two_stage_spy_1dte_2dte_open_to_close_day1"
OPEN_TO_CLOSE_EXIT_REASON = "close_day_1"


def _build_strategy_sets(symbol: str) -> tuple[
    tuple[StrategyConfig, ...],
    tuple[StrategyConfig, ...],
    tuple[StrategyConfig, ...],
]:
    lower = symbol.lower()
    bullish = tuple(
        StrategyConfig(f"{lower}_call_d{delta}_o2c1", symbol, StrategyType.CALENDAR_SPREAD, delta, 0)
        for delta in (40, 50)
    )
    bearish = tuple(
        StrategyConfig(f"bear_{lower}_{side}_d{delta}_o2c1", symbol, strategy_type, delta, 0)
        for side, strategy_type in (
            ("call", StrategyType.CALENDAR_SPREAD),
            ("put", StrategyType.PUT_CALENDAR_SPREAD),
        )
        for delta in (30, 40, 50)
    )
    neutral = tuple(
        StrategyConfig(f"neutral_{lower}_call_d{delta}_o2c1", symbol, StrategyType.CALENDAR_SPREAD, delta, 0)
        for delta in (40, 50)
    )
    return bullish, bearish, neutral


def shift_indicator_rows_to_entry_dates(
    *,
    indicators_by_date: dict[date, dict[str, float | None]],
    entry_dates: list[date],
) -> dict[date, dict[str, float | None]]:
    if not indicators_by_date or not entry_dates:
        return {}
    ordered_indicator_dates = sorted(indicators_by_date)
    shifted: dict[date, dict[str, float | None]] = {}
    for entry_date in sorted(dict.fromkeys(entry_dates)):
        indicator_index = bisect_left(ordered_indicator_dates, entry_date) - 1
        if indicator_index < 0:
            continue
        shifted[entry_date] = indicators_by_date[ordered_indicator_dates[indicator_index]]
    return shifted


def shift_indicator_cache_to_entry_dates(
    *,
    indicator_cache: dict[str, dict[date, dict[str, float | None]]],
    entry_dates: list[date],
) -> dict[str, dict[date, dict[str, float | None]]]:
    return {
        label: shift_indicator_rows_to_entry_dates(
            indicators_by_date=indicators_by_date,
            entry_dates=entry_dates,
        )
        for label, indicators_by_date in indicator_cache.items()
    }


class HistoricalOptionPriceSourceView:
    def __init__(self, store: HistoricalMarketDataStore, *, price_source: str) -> None:
        self._store = store
        self._price_source = price_source

    def list_option_contracts(self, **kwargs):
        return self._store.list_option_contracts(price_source=self._price_source, **kwargs)

    def list_option_contracts_for_expiration(self, **kwargs):
        return self._store.list_option_contracts_for_expiration(price_source=self._price_source, **kwargs)

    def list_option_contracts_for_expirations(self, **kwargs):
        return self._store.list_option_contracts_for_expirations(price_source=self._price_source, **kwargs)

    def list_option_contracts_for_expirations_by_type(self, **kwargs):
        return self._store.list_option_contracts_for_expirations_by_type(price_source=self._price_source, **kwargs)

    def list_available_option_expirations(self, **kwargs):
        return self._store.list_available_option_expirations(**kwargs)

    def list_available_option_expirations_by_type(self, **kwargs):
        return self._store.list_available_option_expirations_by_type(**kwargs)

    def get_option_quote_for_date(self, option_ticker: str, trade_date: date):
        return self._store.get_option_quote_for_date(
            option_ticker,
            trade_date,
            price_source=self._price_source,
        )

    def get_option_quotes_for_date(self, option_tickers: list[str], trade_date: date):
        return self._store.get_option_quotes_for_date(
            option_tickers,
            trade_date,
            price_source=self._price_source,
        )

    def get_option_quote_series(self, option_tickers: list[str], start_date: date, end_date: date):
        return self._store.get_option_quote_series(
            option_tickers,
            start_date,
            end_date,
            price_source=self._price_source,
        )

    def __getattr__(self, name: str):
        return getattr(self._store, name)


class IntradayQuoteSelectionGateway:
    def __init__(
        self,
        gateway: HistoricalOptionGateway,
        quote_cache: IntradayOptionQuoteCache,
        *,
        selection: str,
    ) -> None:
        self._gateway = gateway
        self._quote_cache = quote_cache
        self._selection = selection

    def get_quote(self, option_ticker: str, trade_date: date):
        if self._selection == "open":
            return self._quote_cache.get_open_quote(option_ticker, trade_date)
        if self._selection == "close":
            return self._quote_cache.get_close_quote(option_ticker, trade_date)
        raise ValueError(f"Unsupported intraday quote selection: {self._selection}")

    def get_quotes(self, option_tickers: list[str], trade_date: date):
        return {
            option_ticker: self.get_quote(option_ticker, trade_date)
            for option_ticker in option_tickers
        }

    def __getattr__(self, name: str):
        return getattr(self._gateway, name)


def _open_entry_bar(bar: DailyBar) -> DailyBar:
    return DailyBar(
        trade_date=bar.trade_date,
        open_price=bar.open_price,
        high_price=bar.open_price,
        low_price=bar.open_price,
        close_price=bar.open_price,
        volume=bar.volume,
    )


def _trade_map_row(trade: TradeResult) -> dict[str, object]:
    roi_on_margin_pct = _trade_roi_on_margin_pct(trade)
    return {
        "entry_date": trade.entry_date.isoformat(),
        "exit_date": trade.exit_date.isoformat(),
        "option_ticker": trade.option_ticker,
        "net_pnl": round(float(trade.net_pnl), 4),
        "roi_on_margin_pct": None if roi_on_margin_pct is None else round(roi_on_margin_pct, 4),
        "exit_reason": trade.exit_reason,
    }


def simulate_open_to_close_trade(
    *,
    strategy: StrategyConfig,
    config: BacktestConfig,
    bar: DailyBar,
    open_gateway: HistoricalOptionGateway,
    close_gateway: HistoricalOptionGateway,
    engine: OptionsBacktestEngine,
) -> TradeResult | None:
    strategy_definition = STRATEGY_REGISTRY[config.strategy_type]
    entry_bar = _open_entry_bar(bar)
    try:
        position = strategy_definition.build_position(
            config=config,
            bar=entry_bar,
            bar_index=0,
            option_gateway=open_gateway,
        )
    except DataUnavailableError:
        return None
    if position is None:
        return None

    position.detail_json.setdefault("entry_underlying_close", entry_bar.close_price)
    position.detail_json["execution_policy"] = "open_to_close_day1"
    position.detail_json["entry_option_price_source"] = "open"
    position.detail_json["exit_option_price_source"] = "close"
    assumptions = position.detail_json.get("assumptions")
    if isinstance(assumptions, list):
        assumptions.append("This variant enters on the daily open and exits on the same-day close.")

    tickers = [leg.ticker for leg in position.option_legs]
    close_quotes = close_gateway.get_quotes(tickers, bar.trade_date)
    exit_prices: dict[str, float] = {}
    for leg in position.option_legs:
        quote = close_quotes.get(leg.ticker)
        if quote is None or quote.mid_price is None or not math.isfinite(quote.mid_price) or quote.mid_price <= 0:
            return None
        leg.last_mid = quote.mid_price
        exit_prices[leg.ticker] = quote.mid_price

    for stock_leg in position.stock_legs:
        stock_leg.last_price = bar.close_price
        exit_prices[stock_leg.symbol] = bar.close_price

    exit_value = engine._current_position_value(position, bar.close_price)
    trade, _ = engine._close_position(
        position,
        config,
        exit_value,
        bar.trade_date,
        bar.close_price,
        exit_prices,
        OPEN_TO_CLOSE_EXIT_REASON,
        [],
        set(),
        current_bar_index=0,
        assignment_detail=None,
        trade_warnings=(),
    )
    return trade


def simulate_intraday_open_to_close_trade(
    *,
    strategy: StrategyConfig,
    config: BacktestConfig,
    bar: DailyBar,
    contract_gateway: HistoricalOptionGateway,
    intraday_quote_cache: IntradayOptionQuoteCache,
    engine: OptionsBacktestEngine,
    stop_loss_pct: float | None = None,
    profit_target_pct: float | None = None,
) -> TradeResult | None:
    strategy_definition = STRATEGY_REGISTRY[config.strategy_type]
    entry_bar = _open_entry_bar(bar)
    entry_gateway = IntradayQuoteSelectionGateway(
        contract_gateway,
        intraday_quote_cache,
        selection="open",
    )
    try:
        position = strategy_definition.build_position(
            config=config,
            bar=entry_bar,
            bar_index=0,
            option_gateway=entry_gateway,
        )
    except DataUnavailableError:
        return None
    if position is None:
        return None

    position.detail_json.setdefault("entry_underlying_close", entry_bar.close_price)
    position.detail_json["execution_policy"] = "massive_intraday_open_quote_to_close_quote_day1"
    position.detail_json["entry_option_price_source"] = "massive_intraday_open_quote"
    position.detail_json["exit_option_price_source"] = "massive_intraday_quote"
    if stop_loss_pct is not None:
        position.detail_json["intraday_stop_loss_pct"] = float(stop_loss_pct)
    if profit_target_pct is not None:
        position.detail_json["intraday_profit_target_pct"] = float(profit_target_pct)
    assumptions = position.detail_json.get("assumptions")
    if isinstance(assumptions, list):
        assumptions.append(
            "This variant enters on the first regular-session option quote and exits on an intraday quote path or the same-day closing quote."
        )

    tickers = [leg.ticker for leg in position.option_legs]
    quotes_by_ticker = {
        ticker: intraday_quote_cache.get_regular_session_quotes(ticker, bar.trade_date)
        for ticker in tickers
    }
    if any(not quotes for quotes in quotes_by_ticker.values()):
        return None

    entry_quote_timestamps: dict[str, int | None] = {}
    for leg in position.option_legs:
        first_quote = quotes_by_ticker[leg.ticker][0]
        if first_quote.mid_price is None or not math.isfinite(first_quote.mid_price) or first_quote.mid_price <= 0:
            return None
        leg.last_mid = first_quote.mid_price
        entry_quote_timestamps[leg.ticker] = first_quote.participant_timestamp

    entry_cost = float(engine._entry_value_per_unit(position))
    capital_at_risk = float(position.capital_required_per_unit)
    latest_quotes = {ticker: quotes[0] for ticker, quotes in quotes_by_ticker.items()}
    quote_indexes = {ticker: 1 for ticker in tickers}
    exit_reason = OPEN_TO_CLOSE_EXIT_REASON
    exit_timestamp = None

    if capital_at_risk > 0 and (stop_loss_pct is not None or profit_target_pct is not None):
        event_timestamps = sorted(
            {
                quote.participant_timestamp
                for quotes in quotes_by_ticker.values()
                for quote in quotes[1:]
                if quote.participant_timestamp is not None
            }
        )
        for participant_timestamp in event_timestamps:
            for ticker in tickers:
                quotes = quotes_by_ticker[ticker]
                index = quote_indexes[ticker]
                while index < len(quotes):
                    next_quote = quotes[index]
                    next_timestamp = next_quote.participant_timestamp
                    if next_timestamp is None or next_timestamp > participant_timestamp:
                        break
                    latest_quotes[ticker] = next_quote
                    index += 1
                quote_indexes[ticker] = index
            for leg in position.option_legs:
                quote = latest_quotes[leg.ticker]
                if quote.mid_price is None or not math.isfinite(quote.mid_price) or quote.mid_price <= 0:
                    return None
                leg.last_mid = quote.mid_price
            position_value = float(engine._current_position_value(position, bar.close_price))
            unrealized_pnl_pct = ((position_value - entry_cost) / capital_at_risk) * 100.0
            if stop_loss_pct is not None and unrealized_pnl_pct <= -float(stop_loss_pct):
                exit_reason = "stop_loss"
                exit_timestamp = participant_timestamp
                break
            if profit_target_pct is not None and unrealized_pnl_pct >= float(profit_target_pct):
                exit_reason = "profit_target"
                exit_timestamp = participant_timestamp
                break

    exit_prices: dict[str, float] = {}
    exit_quote_timestamps: dict[str, int | None] = {}
    if exit_reason == OPEN_TO_CLOSE_EXIT_REASON:
        selected_exit_quotes = {
            ticker: quotes[-1]
            for ticker, quotes in quotes_by_ticker.items()
        }
    else:
        selected_exit_quotes = latest_quotes
    for leg in position.option_legs:
        quote = selected_exit_quotes[leg.ticker]
        if quote.mid_price is None or not math.isfinite(quote.mid_price) or quote.mid_price <= 0:
            return None
        leg.last_mid = quote.mid_price
        exit_prices[leg.ticker] = quote.mid_price
        exit_quote_timestamps[leg.ticker] = quote.participant_timestamp

    for stock_leg in position.stock_legs:
        stock_leg.last_price = bar.close_price
        exit_prices[stock_leg.symbol] = bar.close_price

    exit_value = engine._current_position_value(position, bar.close_price)
    trade, _ = engine._close_position(
        position,
        config,
        exit_value,
        bar.trade_date,
        bar.close_price,
        exit_prices,
        exit_reason,
        [],
        set(),
        current_bar_index=0,
        assignment_detail=None,
        trade_warnings=(),
    )
    trade.detail_json["entry_quote_participant_timestamps"] = entry_quote_timestamps
    trade.detail_json["exit_quote_participant_timestamps"] = exit_quote_timestamps
    trade.detail_json["intraday_exit_participant_timestamp"] = exit_timestamp
    trade.detail_json["intraday_exit_mode"] = (
        "regular_session_close_quote"
        if exit_reason == OPEN_TO_CLOSE_EXIT_REASON
        else "intraday_threshold"
    )
    return trade


def precompute_open_to_close_trade_maps(
    *,
    strategies: tuple[StrategyConfig, ...],
    bundle,
    trading_fridays: list[date],
    latest_available_date: date,
    curve,
    start_date: date,
    use_cache: bool,
    worker_count: int,
    cache_root: Path,
) -> dict[str, dict[date, dict[str, object]]]:
    precomputed: dict[str, dict[date, dict[str, object]]] = {}
    uncached_work: list[tuple[int, StrategyConfig, Path]] = []
    for index, strategy in enumerate(strategies, start=1):
        cache_path = (
            cache_root
            / strategy.symbol.lower()
            / f"{start_date.isoformat()}_{latest_available_date.isoformat()}"
            / "trade_maps"
            / f"{strategy.label}.json"
        )
        if use_cache and cache_path.exists():
            cached_payload = json.loads(cache_path.read_text(encoding="utf-8"))
            precomputed[strategy.label] = {
                date.fromisoformat(trade_date): trade_row
                for trade_date, trade_row in cached_payload.get("trade_map", {}).items()
            }
            print(
                f"[precompute {index}/{len(strategies)}] {strategy.label}: "
                f"{len(precomputed[strategy.label])} tradable sessions (cache)"
            )
            continue
        uncached_work.append((index, strategy, cache_path))

    bars_by_date = {
        bar.trade_date: bar
        for bar in bundle.bars
        if start_date <= bar.trade_date <= latest_available_date
    }
    base_store = getattr(bundle.option_gateway, "store", None)
    if not isinstance(base_store, HistoricalMarketDataStore):
        raise TypeError("Expected HistoricalOptionGateway.store to be a HistoricalMarketDataStore instance.")

    def _compute_strategy(item: tuple[int, StrategyConfig, Path]) -> tuple[int, StrategyConfig, Path, dict[date, dict[str, object]]]:
        index, strategy, cache_path = item
        engine = OptionsBacktestEngine()
        open_gateway = HistoricalOptionGateway(
            HistoricalOptionPriceSourceView(base_store, price_source="open"),
            strategy.symbol,
        )
        close_gateway = HistoricalOptionGateway(
            HistoricalOptionPriceSourceView(base_store, price_source="close"),
            strategy.symbol,
        )
        trade_map: dict[date, dict[str, object]] = {}
        for entry_date in trading_fridays:
            bar = bars_by_date.get(entry_date)
            if bar is None:
                continue
            config = _build_calendar_config(
                strategy=strategy,
                entry_date=entry_date,
                latest_available_date=latest_available_date,
                risk_free_curve=curve,
            )
            trade = simulate_open_to_close_trade(
                strategy=strategy,
                config=config,
                bar=bar,
                open_gateway=open_gateway,
                close_gateway=close_gateway,
                engine=engine,
            )
            if trade is not None:
                trade_map[entry_date] = _trade_map_row(trade)
        return index, strategy, cache_path, trade_map

    if uncached_work:
        resolved_worker_count = max(1, min(worker_count, len(uncached_work)))
        if resolved_worker_count == 1:
            computed_results = [_compute_strategy(item) for item in uncached_work]
        else:
            computed_results = []
            with ThreadPoolExecutor(max_workers=resolved_worker_count) as executor:
                futures = {executor.submit(_compute_strategy, item): item for item in uncached_work}
                for future in as_completed(futures):
                    computed_results.append(future.result())
        for index, strategy, cache_path, trade_map in sorted(computed_results, key=lambda item: item[0]):
            precomputed[strategy.label] = trade_map
            if use_cache:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(
                    json.dumps(
                        {
                            "symbol": strategy.symbol,
                            "start_date": start_date.isoformat(),
                            "latest_available_date": latest_available_date.isoformat(),
                            "strategy_label": strategy.label,
                            "execution_policy": "open_to_close_day1",
                            "trade_map": {trade_date.isoformat(): trade_row for trade_date, trade_row in trade_map.items()},
                        },
                        separators=(",", ":"),
                    ),
                    encoding="utf-8",
                )
            print(f"[precompute {index}/{len(strategies)}] {strategy.label}: {len(trade_map)} tradable sessions")
    return precomputed
