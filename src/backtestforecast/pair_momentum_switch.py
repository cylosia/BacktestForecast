from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from itertools import product

from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import BacktestSummary, EquityPointResult, TradeResult
from backtestforecast.market_data.types import DailyBar
from backtestforecast.underlying_rotation import UnderlyingRotationDataset, _PriceHistory


_D0 = Decimal("0")
_INFERRED_SPLIT_SHARE_MULTIPLIERS = (
    0.05,
    0.1,
    0.125,
    0.2,
    0.25,
    1.0 / 3.0,
    0.5,
    2.0,
    3.0,
    4.0,
    5.0,
    8.0,
    10.0,
    20.0,
)
_INFERRED_SPLIT_TOLERANCE = 0.08
_INFERRED_SPLIT_MIN_MOVE = 0.35


def _D(value: float | int) -> Decimal:
    return Decimal(str(value))


@dataclass(frozen=True, slots=True)
class PairMomentumSwitchConfig:
    symbols: tuple[str, str]
    lookback_days: int
    rebalance_frequency_days: int = 1
    trailing_stop_pct: float = 0.0
    require_positive_momentum: bool = False
    position_direction: str = "long"
    invert_ranking: bool = False
    use_raw_execution_prices: bool = False

    def __post_init__(self) -> None:
        normalized = tuple(symbol.upper() for symbol in self.symbols)
        if len(normalized) != 2:
            raise ValueError("symbols must contain exactly 2 entries")
        if len(set(normalized)) != 2:
            raise ValueError("symbols must be unique")
        if self.lookback_days < 1:
            raise ValueError("lookback_days must be >= 1")
        if self.rebalance_frequency_days < 1:
            raise ValueError("rebalance_frequency_days must be >= 1")
        if self.trailing_stop_pct < 0 or self.trailing_stop_pct >= 1:
            raise ValueError("trailing_stop_pct must be between 0 and 1")
        normalized_direction = self.position_direction.lower()
        if normalized_direction not in {"long", "short"}:
            raise ValueError("position_direction must be 'long' or 'short'")
        object.__setattr__(self, "symbols", normalized)
        object.__setattr__(self, "position_direction", normalized_direction)


@dataclass(frozen=True, slots=True)
class PairMomentumSwitchBacktestResult:
    config: PairMomentumSwitchConfig
    summary: BacktestSummary
    trades: tuple[TradeResult, ...]
    equity_curve: tuple[EquityPointResult, ...]
    warnings: tuple[dict[str, str], ...]


@dataclass(frozen=True, slots=True)
class PairMomentumSwitchOptimizationRow:
    config: PairMomentumSwitchConfig
    train_result: PairMomentumSwitchBacktestResult
    validation_result: PairMomentumSwitchBacktestResult | None = None


@dataclass(frozen=True, slots=True)
class PairMomentumSwitchOptimizationResult:
    candidate_count: int
    top_rows: tuple[PairMomentumSwitchOptimizationRow, ...]
    best_config: PairMomentumSwitchConfig
    best_train_result: PairMomentumSwitchBacktestResult
    best_validation_result: PairMomentumSwitchBacktestResult | None


@dataclass(frozen=True, slots=True)
class _SignalPlan:
    ranked_symbols_by_execution_date: dict[date, tuple[str, ...]]


@dataclass(frozen=True, slots=True)
class _PreparedPairDataset:
    trade_dates: tuple[date, ...]
    signal_histories: dict[str, _PriceHistory]
    execution_histories: dict[str, _PriceHistory]
    execution_split_multipliers: dict[str, dict[date, float]]


@dataclass(slots=True)
class _OpenPairPosition:
    symbol: str
    entry_date: date
    entry_price: float
    cost_basis_price: float
    entry_share_quantity: float
    share_quantity: float
    direction: str
    highest_close: float
    lowest_close: float
    last_close: float


def run_pair_momentum_switch_backtest(
    dataset: UnderlyingRotationDataset,
    *,
    config: PairMomentumSwitchConfig,
    start_date: date,
    end_date: date,
    starting_equity: float = 100_000.0,
    risk_free_rate: float = 0.0,
    signal_plan: _SignalPlan | None = None,
) -> PairMomentumSwitchBacktestResult:
    prepared_dataset = _prepare_pair_dataset(
        dataset,
        config.symbols,
        use_raw_execution_prices=config.use_raw_execution_prices,
    )
    trade_dates = [item for item in prepared_dataset.trade_dates if start_date <= item <= end_date]
    warnings: list[dict[str, str]] = []
    if not trade_dates:
        summary = build_summary(
            starting_equity,
            starting_equity,
            [],
            [],
            risk_free_rate=risk_free_rate,
            warnings=warnings,
        )
        return PairMomentumSwitchBacktestResult(
            config=config,
            summary=summary,
            trades=(),
            equity_curve=(),
            warnings=tuple(warnings),
        )

    plan = signal_plan or _build_signal_plan(
        prepared_dataset,
        start_date=start_date,
        end_date=end_date,
        config=config,
    )

    cash = starting_equity
    open_position: _OpenPairPosition | None = None
    trades: list[TradeResult] = []
    equity_curve: list[EquityPointResult] = []
    running_peak = starting_equity

    for trade_date in trade_dates:
        if open_position is not None:
            share_multiplier = prepared_dataset.execution_split_multipliers.get(open_position.symbol, {}).get(trade_date)
            if share_multiplier is not None:
                _apply_split_multiplier(open_position, share_multiplier)

        target_symbols = plan.ranked_symbols_by_execution_date.get(trade_date)
        if target_symbols is not None:
            selected_symbol = _select_tradeable_symbol(prepared_dataset.execution_histories, trade_date, target_symbols)
            if open_position is not None and (selected_symbol is None or open_position.symbol != selected_symbol):
                current_bar = prepared_dataset.execution_histories[open_position.symbol].bar_on(trade_date)
                exit_price = current_bar.open_price if current_bar is not None else open_position.last_close
                cash += _close_position(
                    open_position,
                    exit_date=trade_date,
                    exit_price=exit_price,
                    exit_reason="cash_filter" if selected_symbol is None else "switch",
                    trades=trades,
                )
                open_position = None
            if selected_symbol is not None and open_position is None:
                target_bar = prepared_dataset.execution_histories[selected_symbol].bar_on(trade_date)
                if target_bar is not None and target_bar.open_price > 0 and cash > 0:
                    share_quantity = cash / target_bar.open_price
                    if config.position_direction == "long":
                        invested_amount = share_quantity * target_bar.open_price
                        cash -= invested_amount
                    open_position = _OpenPairPosition(
                        symbol=selected_symbol,
                        entry_date=trade_date,
                        entry_price=target_bar.open_price,
                        cost_basis_price=target_bar.open_price,
                        entry_share_quantity=share_quantity,
                        share_quantity=share_quantity,
                        direction=config.position_direction,
                        highest_close=target_bar.open_price,
                        lowest_close=target_bar.open_price,
                        last_close=target_bar.open_price,
                    )

        if config.trailing_stop_pct > 0 and open_position is not None:
            bar = prepared_dataset.execution_histories[open_position.symbol].bar_on(trade_date)
            if bar is None:
                cash += _close_position(
                    open_position,
                    exit_date=trade_date,
                    exit_price=open_position.last_close,
                    exit_reason="data_unavailable",
                    trades=trades,
                )
                open_position = None
            else:
                stop_price = _trailing_stop_price(open_position, config.trailing_stop_pct)
                exit_price = _trailing_stop_exit_price(stop_price, bar, direction=open_position.direction)
                if exit_price is not None:
                    cash += _close_position(
                        open_position,
                        exit_date=trade_date,
                        exit_price=exit_price,
                        exit_reason="trailing_stop",
                        trades=trades,
                        stop_price=stop_price,
                    )
                    open_position = None

        position_value = 0.0
        if open_position is not None:
            bar = prepared_dataset.execution_histories[open_position.symbol].bar_on(trade_date)
            if bar is not None:
                open_position.last_close = bar.close_price
                open_position.highest_close = max(open_position.highest_close, bar.close_price)
                open_position.lowest_close = min(open_position.lowest_close, bar.close_price)
            position_value = _position_mark_to_market_value(open_position)

        equity = cash + position_value
        running_peak = max(running_peak, equity)
        drawdown_pct = ((running_peak - equity) / running_peak * 100.0) if running_peak > 0 else 0.0
        equity_curve.append(
            EquityPointResult(
                trade_date=trade_date,
                equity=_D(equity),
                cash=_D(cash),
                position_value=_D(position_value),
                drawdown_pct=_D(drawdown_pct),
            )
        )

    final_trade_date = trade_dates[-1]
    if open_position is not None:
        final_bar = prepared_dataset.execution_histories[open_position.symbol].bar_on(final_trade_date)
        exit_price = final_bar.close_price if final_bar is not None else open_position.last_close
        cash += _close_position(
            open_position,
            exit_date=final_trade_date,
            exit_price=exit_price,
            exit_reason="backtest_end",
            trades=trades,
        )

    summary = build_summary(
        starting_equity,
        cash,
        trades,
        equity_curve,
        risk_free_rate=risk_free_rate,
        warnings=warnings,
    )
    if not trades:
        warnings.append(
            {
                "code": "no_trades",
                "message": "No pair momentum switch trades were generated for this parameter set.",
            }
        )
    return PairMomentumSwitchBacktestResult(
        config=config,
        summary=summary,
        trades=tuple(trades),
        equity_curve=tuple(equity_curve),
        warnings=tuple(warnings),
    )


def optimize_pair_momentum_switch(
    dataset: UnderlyingRotationDataset,
    *,
    symbols: tuple[str, str],
    lookback_days: tuple[int, ...],
    rebalance_frequency_days: int | tuple[int, ...],
    position_direction: str = "long",
    invert_ranking: bool = False,
    use_raw_execution_prices: bool = False,
    train_start: date,
    train_end: date,
    validation_start: date | None = None,
    validation_end: date | None = None,
    starting_equity: float = 100_000.0,
    risk_free_rate: float = 0.0,
    top_validation_count: int = 20,
    objective: str = "roi",
    trailing_stop_pcts: tuple[float, ...] = (0.0,),
    require_positive_momentum_values: tuple[bool, ...] = (False,),
) -> PairMomentumSwitchOptimizationResult:
    rebalance_values = (
        (rebalance_frequency_days,)
        if isinstance(rebalance_frequency_days, int)
        else tuple(sorted({item for item in rebalance_frequency_days if item >= 1}))
    )
    configs = tuple(
        PairMomentumSwitchConfig(
            symbols=symbols,
            lookback_days=lookback_day,
            rebalance_frequency_days=rebalance_frequency,
            trailing_stop_pct=trailing_stop_pct,
            require_positive_momentum=require_positive_momentum,
            position_direction=position_direction,
            invert_ranking=invert_ranking,
            use_raw_execution_prices=use_raw_execution_prices,
        )
        for lookback_day, rebalance_frequency, trailing_stop_pct, require_positive_momentum in product(
            sorted({item for item in lookback_days if item >= 1}),
            rebalance_values,
            tuple(sorted({item for item in trailing_stop_pcts if 0 <= item < 1})),
            tuple(dict.fromkeys(require_positive_momentum_values)),
        )
    )
    rows: list[PairMomentumSwitchOptimizationRow] = []
    for config in configs:
        train_result = run_pair_momentum_switch_backtest(
            dataset,
            config=config,
            start_date=train_start,
            end_date=train_end,
            starting_equity=starting_equity,
            risk_free_rate=risk_free_rate,
        )
        rows.append(PairMomentumSwitchOptimizationRow(config=config, train_result=train_result))

    train_ranked = sorted(
        rows,
        key=lambda row: _summary_sort_key(row.train_result.summary, objective=objective),
        reverse=True,
    )
    shortlisted = train_ranked[: max(1, top_validation_count)]

    if validation_start is not None and validation_end is not None:
        validated_rows: list[PairMomentumSwitchOptimizationRow] = []
        for row in shortlisted:
            validation_result = run_pair_momentum_switch_backtest(
                dataset,
                config=row.config,
                start_date=validation_start,
                end_date=validation_end,
                starting_equity=starting_equity,
                risk_free_rate=risk_free_rate,
            )
            validated_rows.append(
                PairMomentumSwitchOptimizationRow(
                    config=row.config,
                    train_result=row.train_result,
                    validation_result=validation_result,
                )
            )
        top_rows = tuple(
            sorted(
                validated_rows,
                key=lambda row: _summary_sort_key(
                    row.validation_result.summary if row.validation_result is not None else row.train_result.summary,
                    objective=objective,
                ),
                reverse=True,
            )
        )
        best_row = top_rows[0]
    else:
        top_rows = tuple(train_ranked)
        best_row = top_rows[0]

    return PairMomentumSwitchOptimizationResult(
        candidate_count=len(configs),
        top_rows=top_rows,
        best_config=best_row.config,
        best_train_result=best_row.train_result,
        best_validation_result=best_row.validation_result,
    )


def _build_signal_plan(
    dataset: _PreparedPairDataset,
    *,
    start_date: date,
    end_date: date,
    config: PairMomentumSwitchConfig,
) -> _SignalPlan:
    trade_dates = [item for item in dataset.trade_dates if start_date <= item <= end_date]
    ranked_symbols_by_execution_date: dict[date, tuple[str, ...]] = {}
    if len(trade_dates) < 2:
        return _SignalPlan(ranked_symbols_by_execution_date)

    for signal_index in range(0, len(trade_dates) - 1, config.rebalance_frequency_days):
        signal_date = trade_dates[signal_index]
        execution_date = trade_dates[signal_index + 1]
        scored_symbols: list[tuple[float, str]] = []
        for symbol in config.symbols:
            history = dataset.signal_histories.get(symbol)
            if history is None:
                continue
            score = _lookback_return(history.dates, history.closes, signal_date, config.lookback_days)
            if score is None:
                continue
            scored_symbols.append((score, symbol))
        scored_symbols.sort(key=lambda item: (item[0], item[1]) if config.invert_ranking else (-item[0], item[1]))
        if scored_symbols:
            selected_score = scored_symbols[0][0]
            if config.require_positive_momentum and _selected_symbol_should_stay_in_cash(selected_score, config):
                ranked_symbols_by_execution_date[execution_date] = ()
            else:
                ranked_symbols_by_execution_date[execution_date] = tuple(symbol for _score, symbol in scored_symbols)
    return _SignalPlan(ranked_symbols_by_execution_date)


def _lookback_return(
    dates: tuple[date, ...],
    closes: tuple[float, ...],
    signal_date: date,
    lookback_days: int,
) -> float | None:
    signal_index = bisect_right(dates, signal_date) - 1
    if signal_index < 0 or dates[signal_index] != signal_date:
        return None
    base_index = signal_index - lookback_days
    if base_index < 0:
        return None
    base_close = closes[base_index]
    current_close = closes[signal_index]
    if base_close <= 0 or current_close <= 0:
        return None
    return (current_close / base_close) - 1.0


def _select_tradeable_symbol(
    execution_histories: dict[str, _PriceHistory],
    trade_date: date,
    ranked_symbols: tuple[str, ...],
) -> str | None:
    for symbol in ranked_symbols:
        history = execution_histories.get(symbol)
        if history is None:
            continue
        bar = history.bar_on(trade_date)
        if bar is None or bar.open_price <= 0:
            continue
        return symbol
    return None


def _close_position(
    position: _OpenPairPosition,
    *,
    exit_date: date,
    exit_price: float,
    exit_reason: str,
    trades: list[TradeResult],
    stop_price: float | None = None,
) -> float:
    gross_pnl = _position_pnl(position, exit_price)
    trades.append(
        TradeResult(
            option_ticker=position.symbol,
            strategy_type="pair_momentum_switch",
            underlying_symbol=position.symbol,
            entry_date=position.entry_date,
            exit_date=exit_date,
            expiration_date=exit_date,
            quantity=1,
            dte_at_open=0,
            holding_period_days=max((exit_date - position.entry_date).days, 0),
            entry_underlying_close=_D(position.entry_price),
            exit_underlying_close=_D(exit_price),
            entry_mid=_D(position.entry_price / 100.0),
            exit_mid=_D(exit_price / 100.0),
            gross_pnl=_D(gross_pnl),
            net_pnl=_D(gross_pnl),
            total_commissions=_D0,
            entry_reason="momentum_switch",
            exit_reason=exit_reason,
            detail_json={
                "entry_share_quantity": position.entry_share_quantity,
                "share_quantity": position.share_quantity,
                "entry_value": position.entry_share_quantity * position.entry_price,
                "exit_value": position.share_quantity * exit_price,
                "position_direction": position.direction,
                "trailing_stop_price": stop_price,
            },
        )
    )
    if position.direction == "short":
        return gross_pnl
    return position.share_quantity * exit_price


def _trailing_stop_price(position: _OpenPairPosition, trailing_stop_pct: float) -> float:
    if position.direction == "short":
        return position.lowest_close * (1.0 + trailing_stop_pct)
    return position.highest_close * (1.0 - trailing_stop_pct)


def _trailing_stop_exit_price(stop_price: float, bar, *, direction: str) -> float | None:
    if stop_price <= 0:
        return None
    if direction == "short":
        if bar.open_price >= stop_price:
            return bar.open_price
        if bar.high_price >= stop_price:
            return stop_price
        return None
    if bar.open_price <= stop_price:
        return bar.open_price
    if bar.low_price <= stop_price:
        return stop_price
    return None


def _summary_sort_key(summary: BacktestSummary, *, objective: str) -> tuple[float, float, float]:
    sharpe = summary.sharpe_ratio if summary.sharpe_ratio is not None else float("-inf")
    if objective == "sharpe":
        return (sharpe, summary.total_roi_pct, -summary.max_drawdown_pct)
    if objective == "roi":
        return (summary.total_roi_pct, sharpe, -summary.max_drawdown_pct)
    raise ValueError(f"Unsupported objective: {objective}")


def _selected_symbol_should_stay_in_cash(selected_score: float, config: PairMomentumSwitchConfig) -> bool:
    if config.invert_ranking:
        return selected_score >= 0
    return selected_score <= 0


def _position_pnl(position: _OpenPairPosition, exit_price: float) -> float:
    if position.direction == "short":
        return (position.cost_basis_price - exit_price) * position.share_quantity
    return (exit_price - position.cost_basis_price) * position.share_quantity


def _position_mark_to_market_value(position: _OpenPairPosition) -> float:
    if position.direction == "short":
        return _position_pnl(position, position.last_close)
    return position.share_quantity * position.last_close


def _prepare_pair_dataset(
    dataset: UnderlyingRotationDataset,
    symbols: tuple[str, str],
    *,
    use_raw_execution_prices: bool = False,
) -> _PreparedPairDataset:
    signal_histories: dict[str, _PriceHistory] = {}
    for symbol in symbols:
        history = dataset.histories.get(symbol)
        if history is None:
            continue
        signal_histories[symbol] = _normalize_history_for_inferred_splits(history)

    execution_split_multipliers: dict[str, dict[date, float]] = {}
    if not use_raw_execution_prices:
        return _PreparedPairDataset(
            trade_dates=dataset.trade_dates,
            signal_histories=signal_histories,
            execution_histories=signal_histories,
            execution_split_multipliers=execution_split_multipliers,
        )

    execution_histories: dict[str, _PriceHistory] = {}
    for symbol in symbols:
        signal_history = signal_histories.get(symbol)
        raw_history = dataset.raw_histories.get(symbol)
        if signal_history is None or raw_history is None:
            raise ValueError(f"Raw execution history unavailable for {symbol}")
        execution_histories[symbol] = raw_history
        execution_split_multipliers[symbol] = _build_execution_split_multipliers(signal_history, raw_history)

    return _PreparedPairDataset(
        trade_dates=dataset.trade_dates,
        signal_histories=signal_histories,
        execution_histories=execution_histories,
        execution_split_multipliers=execution_split_multipliers,
    )


def _apply_split_multiplier(position: _OpenPairPosition, share_multiplier: float) -> None:
    if share_multiplier <= 0 or abs(share_multiplier - 1.0) <= 1e-12:
        return
    position.share_quantity *= share_multiplier
    position.cost_basis_price /= share_multiplier
    position.highest_close /= share_multiplier
    position.lowest_close /= share_multiplier
    position.last_close /= share_multiplier


def _build_execution_split_multipliers(
    signal_history: _PriceHistory,
    execution_history: _PriceHistory,
) -> dict[date, float]:
    shared_dates = tuple(
        trade_date
        for trade_date in execution_history.dates
        if trade_date in signal_history.bars_by_date
    )
    if len(shared_dates) < 2:
        return {}

    multipliers: dict[date, float] = {}
    previous_date = shared_dates[0]
    for trade_date in shared_dates[1:]:
        previous_signal_bar = signal_history.bar_on(previous_date)
        previous_execution_bar = execution_history.bar_on(previous_date)
        signal_bar = signal_history.bar_on(trade_date)
        execution_bar = execution_history.bar_on(trade_date)
        if (
            previous_signal_bar is None
            or previous_execution_bar is None
            or signal_bar is None
            or execution_bar is None
        ):
            previous_date = trade_date
            continue
        share_multiplier = _infer_ratio_based_split_share_multiplier(
            previous_signal_close=previous_signal_bar.close_price,
            previous_execution_close=previous_execution_bar.close_price,
            current_signal_open=signal_bar.open_price,
            current_execution_open=execution_bar.open_price,
            current_signal_close=signal_bar.close_price,
            current_execution_close=execution_bar.close_price,
        )
        if share_multiplier is not None:
            multipliers[trade_date] = share_multiplier
        previous_date = trade_date
    return multipliers


def _infer_ratio_based_split_share_multiplier(
    *,
    previous_signal_close: float,
    previous_execution_close: float,
    current_signal_open: float,
    current_execution_open: float,
    current_signal_close: float,
    current_execution_close: float,
) -> float | None:
    if (
        previous_signal_close <= 0
        or previous_execution_close <= 0
        or current_signal_open <= 0
        or current_execution_open <= 0
        or current_signal_close <= 0
        or current_execution_close <= 0
    ):
        return None

    previous_ratio = previous_execution_close / previous_signal_close
    current_open_ratio = current_execution_open / current_signal_open
    current_close_ratio = current_execution_close / current_signal_close
    if current_open_ratio <= 0 or current_close_ratio <= 0:
        return None

    open_multiplier = previous_ratio / current_open_ratio
    close_multiplier = previous_ratio / current_close_ratio
    if max(abs(open_multiplier - 1.0), abs(close_multiplier - 1.0)) < _INFERRED_SPLIT_MIN_MOVE:
        return None

    best_multiplier: float | None = None
    best_error: float | None = None
    for candidate in _INFERRED_SPLIT_SHARE_MULTIPLIERS:
        open_error = abs(open_multiplier - candidate) / candidate
        close_error = abs(close_multiplier - candidate) / candidate
        candidate_error = max(open_error, close_error)
        if best_error is None or candidate_error < best_error:
            best_multiplier = candidate
            best_error = candidate_error

    if best_multiplier is None or best_error is None or best_error > _INFERRED_SPLIT_TOLERANCE:
        return None
    return best_multiplier


def _normalize_history_for_inferred_splits(history: _PriceHistory) -> _PriceHistory:
    if len(history.bars) < 2:
        return history

    adjusted_bars: list[DailyBar] = []
    cumulative_price_scale = 1.0
    previous_bar: DailyBar | None = None
    split_count = 0

    for bar in history.bars:
        if previous_bar is not None:
            inferred_multiplier = _infer_split_share_multiplier(
                previous_close=previous_bar.close_price,
                current_open=bar.open_price,
                current_close=bar.close_price,
            )
            if inferred_multiplier is not None:
                cumulative_price_scale *= inferred_multiplier
                split_count += 1

        adjusted_bar = DailyBar(
            trade_date=bar.trade_date,
            open_price=bar.open_price * cumulative_price_scale,
            high_price=bar.high_price * cumulative_price_scale,
            low_price=bar.low_price * cumulative_price_scale,
            close_price=bar.close_price * cumulative_price_scale,
            volume=bar.volume,
        )
        adjusted_bars.append(adjusted_bar)
        previous_bar = bar

    if split_count == 0:
        return history

    ordered_bars = tuple(adjusted_bars)
    return _PriceHistory(
        symbol=history.symbol,
        bars=ordered_bars,
        dates=tuple(item.trade_date for item in ordered_bars),
        bars_by_date={item.trade_date: item for item in ordered_bars},
        closes=tuple(item.close_price for item in ordered_bars),
    )


def _infer_split_share_multiplier(
    *,
    previous_close: float,
    current_open: float,
    current_close: float,
) -> float | None:
    if previous_close <= 0 or current_open <= 0 or current_close <= 0:
        return None

    open_multiplier = previous_close / current_open
    close_multiplier = previous_close / current_close
    if max(abs(open_multiplier - 1.0), abs(close_multiplier - 1.0)) < _INFERRED_SPLIT_MIN_MOVE:
        return None

    best_multiplier: float | None = None
    best_error: float | None = None
    for candidate in _INFERRED_SPLIT_SHARE_MULTIPLIERS:
        open_error = abs(open_multiplier - candidate) / candidate
        close_error = abs(close_multiplier - candidate) / candidate
        candidate_error = max(open_error, close_error)
        if best_error is None or candidate_error < best_error:
            best_multiplier = candidate
            best_error = candidate_error

    if best_multiplier is None or best_error is None or best_error > _INFERRED_SPLIT_TOLERANCE:
        return None
    return best_multiplier
