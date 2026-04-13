from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from itertools import product
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import BacktestSummary, EquityPointResult, TradeResult
from backtestforecast.market_data.types import DailyBar
from backtestforecast.models import HistoricalUnderlyingDayBar, HistoricalUnderlyingRawDayBar


_D0 = Decimal("0")
_D100 = Decimal("100")
_TRADING_DAYS_PER_YEAR = 252


def _D(value: float | int) -> Decimal:
    return Decimal(str(value))


@dataclass(frozen=True, slots=True)
class UnderlyingRotationConfig:
    portfolio_size: int
    lookback_days: tuple[int, int, int]
    lookback_weights: tuple[float, float, float]
    trailing_stop_pct: float
    rebalance_frequency_days: int

    def __post_init__(self) -> None:
        if self.portfolio_size < 1:
            raise ValueError("portfolio_size must be >= 1")
        if self.rebalance_frequency_days < 1:
            raise ValueError("rebalance_frequency_days must be >= 1")
        if len(self.lookback_days) != 3:
            raise ValueError("lookback_days must contain exactly 3 values")
        if len(self.lookback_weights) != 3:
            raise ValueError("lookback_weights must contain exactly 3 values")
        if tuple(sorted(self.lookback_days)) != self.lookback_days:
            raise ValueError("lookback_days must be sorted ascending")
        if len(set(self.lookback_days)) != 3:
            raise ValueError("lookback_days must be unique")
        if any(item <= 0 for item in self.lookback_days):
            raise ValueError("lookback_days must be positive")
        if self.trailing_stop_pct < 0 or self.trailing_stop_pct >= 1:
            raise ValueError("trailing_stop_pct must be between 0 and 1")
        if any(weight < 0 for weight in self.lookback_weights):
            raise ValueError("lookback_weights cannot be negative")
        weight_sum = sum(self.lookback_weights)
        if abs(weight_sum - 1.0) > 1e-9:
            raise ValueError("lookback_weights must sum to 1.0")

    @property
    def ranking_key(self) -> tuple[tuple[int, int, int], tuple[float, float, float], int]:
        return (self.lookback_days, self.lookback_weights, self.rebalance_frequency_days)


@dataclass(frozen=True, slots=True)
class UnderlyingUniverseFilter:
    min_training_bars: int = 126
    min_training_avg_dollar_volume: float = 1_000_000.0
    min_training_close_price: float = 5.0


@dataclass(frozen=True, slots=True)
class UnderlyingUniverseMember:
    symbol: str
    training_bar_count: int
    avg_dollar_volume: float
    min_close_price: float


@dataclass(frozen=True, slots=True)
class UnderlyingRotationSearchSpace:
    portfolio_sizes: tuple[int, ...]
    lookback_triplets: tuple[tuple[int, int, int], ...]
    weight_triplets: tuple[tuple[float, float, float], ...]
    trailing_stop_pcts: tuple[float, ...]
    rebalance_frequencies: tuple[int, ...]

    def iter_configs(self) -> tuple[UnderlyingRotationConfig, ...]:
        configs = []
        for portfolio_size, lookbacks, weights, stop_pct, rebalance_frequency in product(
            self.portfolio_sizes,
            self.lookback_triplets,
            self.weight_triplets,
            self.trailing_stop_pcts,
            self.rebalance_frequencies,
        ):
            configs.append(
                UnderlyingRotationConfig(
                    portfolio_size=portfolio_size,
                    lookback_days=lookbacks,
                    lookback_weights=weights,
                    trailing_stop_pct=stop_pct,
                    rebalance_frequency_days=rebalance_frequency,
                )
            )
        return tuple(configs)

    @property
    def max_portfolio_size(self) -> int:
        return max(self.portfolio_sizes, default=0)


@dataclass(frozen=True, slots=True)
class UnderlyingRotationBacktestResult:
    config: UnderlyingRotationConfig
    summary: BacktestSummary
    trades: tuple[TradeResult, ...]
    equity_curve: tuple[EquityPointResult, ...]
    warnings: tuple[dict[str, str], ...]


@dataclass(frozen=True, slots=True)
class UnderlyingRotationOptimizationRow:
    config: UnderlyingRotationConfig
    train_result: UnderlyingRotationBacktestResult
    validation_result: UnderlyingRotationBacktestResult | None = None


@dataclass(frozen=True, slots=True)
class UnderlyingRotationOptimizationResult:
    universe_size: int
    candidate_count: int
    top_rows: tuple[UnderlyingRotationOptimizationRow, ...]
    best_config: UnderlyingRotationConfig
    best_train_result: UnderlyingRotationBacktestResult
    best_validation_result: UnderlyingRotationBacktestResult | None


@dataclass(frozen=True, slots=True)
class UnderlyingRotationWalkForwardSplit:
    train_start: date
    train_end: date
    validation_start: date
    validation_end: date


@dataclass(frozen=True, slots=True)
class UnderlyingRotationRollingSplitResult:
    split: UnderlyingRotationWalkForwardSplit
    universe_size: int
    best_row: UnderlyingRotationOptimizationRow
    all_rows: tuple[UnderlyingRotationOptimizationRow, ...]


@dataclass(frozen=True, slots=True)
class UnderlyingRotationRollingConfigStats:
    config: UnderlyingRotationConfig
    median_validation_roi_pct: float
    average_validation_roi_pct: float
    median_validation_max_drawdown_pct: float
    average_validation_sharpe: float | None
    positive_validation_split_count: int
    within_drawdown_cap_split_count: int
    split_count: int
    split_metrics: tuple[dict[str, float | int | None], ...]


@dataclass(frozen=True, slots=True)
class UnderlyingRotationRollingDecision:
    action: str
    challenger: UnderlyingRotationRollingConfigStats
    incumbent: UnderlyingRotationRollingConfigStats | None
    median_validation_roi_improvement_pct: float | None
    reason: str


@dataclass(frozen=True, slots=True)
class _PriceHistory:
    symbol: str
    bars: tuple[DailyBar, ...]
    dates: tuple[date, ...]
    bars_by_date: dict[date, DailyBar]
    closes: tuple[float, ...]

    def bar_on(self, trade_date: date) -> DailyBar | None:
        return self.bars_by_date.get(trade_date)

    def close_on_or_before(self, target_date: date) -> float | None:
        idx = bisect_right(self.dates, target_date) - 1
        if idx < 0:
            return None
        return self.closes[idx]


@dataclass(frozen=True, slots=True)
class UnderlyingRotationDataset:
    start_date: date
    end_date: date
    trade_dates: tuple[date, ...]
    histories: dict[str, _PriceHistory]
    universe_members: dict[str, UnderlyingUniverseMember]
    raw_histories: dict[str, _PriceHistory] = field(default_factory=dict)

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self.histories))


@dataclass(slots=True)
class _OpenPosition:
    symbol: str
    entry_date: date
    entry_price: float
    share_quantity: float
    highest_close: float
    last_close: float


@dataclass(frozen=True, slots=True)
class _RankingPlan:
    ranked_symbols_by_execution_date: dict[date, tuple[str, ...]]


def load_rotation_dataset(
    session: Session,
    *,
    train_start: date,
    train_end: date,
    end_date: date,
    max_lookback_days: int,
    universe_filter: UnderlyingUniverseFilter | None = None,
    symbols: tuple[str, ...] | None = None,
    batch_size: int = 500,
    include_raw_histories: bool = False,
) -> UnderlyingRotationDataset:
    filters = universe_filter or UnderlyingUniverseFilter()
    warmup_start = train_start - timedelta(days=max_lookback_days + 31)
    universe_stmt = (
        select(
            HistoricalUnderlyingDayBar.symbol,
            func.count().label("bar_count"),
            func.avg(HistoricalUnderlyingDayBar.close_price * HistoricalUnderlyingDayBar.volume).label("avg_dollar_volume"),
            func.min(HistoricalUnderlyingDayBar.close_price).label("min_close_price"),
        )
        .where(
            HistoricalUnderlyingDayBar.trade_date >= train_start,
            HistoricalUnderlyingDayBar.trade_date <= train_end,
        )
        .group_by(HistoricalUnderlyingDayBar.symbol)
        .having(
            func.count() >= filters.min_training_bars,
            func.avg(HistoricalUnderlyingDayBar.close_price * HistoricalUnderlyingDayBar.volume)
            >= filters.min_training_avg_dollar_volume,
            func.min(HistoricalUnderlyingDayBar.close_price) >= filters.min_training_close_price,
        )
        .order_by(HistoricalUnderlyingDayBar.symbol)
    )
    if symbols:
        universe_stmt = universe_stmt.where(HistoricalUnderlyingDayBar.symbol.in_(symbols))

    universe_members: dict[str, UnderlyingUniverseMember] = {}
    for symbol, bar_count, avg_dollar_volume, min_close_price in session.execute(universe_stmt):
        if symbol is None:
            continue
        normalized_symbol = str(symbol).upper()
        universe_members[normalized_symbol] = UnderlyingUniverseMember(
            symbol=normalized_symbol,
            training_bar_count=int(bar_count),
            avg_dollar_volume=float(avg_dollar_volume),
            min_close_price=float(min_close_price),
        )

    selected_symbols = tuple(sorted(universe_members))
    histories = _load_price_histories(
        session,
        model=HistoricalUnderlyingDayBar,
        symbols=selected_symbols,
        start_date=warmup_start,
        end_date=end_date,
        batch_size=batch_size,
    )
    trade_dates: set[date] = set()
    for symbol, history in histories.items():
        dates = history.dates
        if dates[-1] < train_start:
            continue
        trade_dates.update(item for item in dates if train_start <= item <= end_date)

    filtered_members = {symbol: member for symbol, member in universe_members.items() if symbol in histories}
    raw_histories = (
        _load_price_histories(
            session,
            model=HistoricalUnderlyingRawDayBar,
            symbols=tuple(sorted(filtered_members)),
            start_date=warmup_start,
            end_date=end_date,
            batch_size=batch_size,
        )
        if include_raw_histories
        else {}
    )
    return UnderlyingRotationDataset(
        start_date=train_start,
        end_date=end_date,
        trade_dates=tuple(sorted(trade_dates)),
        histories=histories,
        universe_members=filtered_members,
        raw_histories=raw_histories,
    )


def _load_price_histories(
    session: Session,
    *,
    model: type[HistoricalUnderlyingDayBar] | type[HistoricalUnderlyingRawDayBar],
    symbols: tuple[str, ...],
    start_date: date,
    end_date: date,
    batch_size: int,
) -> dict[str, _PriceHistory]:
    histories_by_symbol: dict[str, list[DailyBar]] = defaultdict(list)
    for chunk_start in range(0, len(symbols), max(1, batch_size)):
        symbol_chunk = symbols[chunk_start : chunk_start + batch_size]
        if not symbol_chunk:
            continue
        data_stmt = (
            select(
                model.symbol,
                model.trade_date,
                model.open_price,
                model.high_price,
                model.low_price,
                model.close_price,
                model.volume,
            )
            .where(
                model.symbol.in_(symbol_chunk),
                model.trade_date >= start_date,
                model.trade_date <= end_date,
            )
            .order_by(model.symbol, model.trade_date)
        )
        for symbol, trade_date, open_price, high_price, low_price, close_price, volume in session.execute(data_stmt):
            histories_by_symbol[str(symbol).upper()].append(
                DailyBar(
                    trade_date=trade_date,
                    open_price=float(open_price),
                    high_price=float(high_price),
                    low_price=float(low_price),
                    close_price=float(close_price),
                    volume=float(volume),
                )
            )

    histories: dict[str, _PriceHistory] = {}
    for symbol, bars in histories_by_symbol.items():
        ordered_bars = tuple(sorted(bars, key=lambda item: item.trade_date))
        if not ordered_bars:
            continue
        histories[symbol] = _PriceHistory(
            symbol=symbol,
            bars=ordered_bars,
            dates=tuple(item.trade_date for item in ordered_bars),
            bars_by_date={item.trade_date: item for item in ordered_bars},
            closes=tuple(item.close_price for item in ordered_bars),
        )
    return histories


def build_trailing_annual_walk_forward_splits(
    *,
    validation_start_year: int,
    validation_end_year: int,
    train_years: int = 5,
    validation_years: int = 1,
    step_years: int = 1,
) -> tuple[UnderlyingRotationWalkForwardSplit, ...]:
    if train_years < 1:
        raise ValueError("train_years must be >= 1")
    if validation_years < 1:
        raise ValueError("validation_years must be >= 1")
    if step_years < 1:
        raise ValueError("step_years must be >= 1")
    if validation_end_year < validation_start_year:
        raise ValueError("validation_end_year must be >= validation_start_year")

    splits: list[UnderlyingRotationWalkForwardSplit] = []
    validation_year = validation_start_year
    while validation_year <= validation_end_year:
        train_start_year = validation_year - train_years
        train_end_year = validation_year - 1
        split_validation_end_year = validation_year + validation_years - 1
        if split_validation_end_year > validation_end_year:
            break
        splits.append(
            UnderlyingRotationWalkForwardSplit(
                train_start=date(train_start_year, 1, 1),
                train_end=date(train_end_year, 12, 31),
                validation_start=date(validation_year, 1, 1),
                validation_end=date(split_validation_end_year, 12, 31),
            )
        )
        validation_year += step_years
    return tuple(splits)


def optimize_underlying_rotation(
    dataset: UnderlyingRotationDataset,
    *,
    search_space: UnderlyingRotationSearchSpace,
    train_start: date,
    train_end: date,
    validation_start: date,
    validation_end: date,
    starting_equity: float = 100_000.0,
    risk_free_rate: float = 0.0,
    top_validation_count: int = 20,
    ranking_buffer: int | None = None,
    objective: str = "sharpe",
    max_drawdown_pct_cap: float | None = None,
) -> UnderlyingRotationOptimizationResult:
    if objective not in {"sharpe", "roi"}:
        raise ValueError("objective must be 'sharpe' or 'roi'")
    configs = search_space.iter_configs()
    max_ranked_symbols = ranking_buffer or max(50, search_space.max_portfolio_size * 3)
    train_rows: list[UnderlyingRotationOptimizationRow] = []
    grouped_configs: dict[tuple[tuple[int, int, int], tuple[float, float, float], int], list[UnderlyingRotationConfig]] = defaultdict(list)
    for config in configs:
        grouped_configs[config.ranking_key].append(config)

    for ranking_key, ranking_configs in grouped_configs.items():
        lookbacks, weights, rebalance_frequency = ranking_key
        plan = _build_ranking_plan(
            dataset,
            start_date=train_start,
            end_date=train_end,
            lookback_days=lookbacks,
            lookback_weights=weights,
            rebalance_frequency_days=rebalance_frequency,
            max_ranked_symbols=max_ranked_symbols,
        )
        for config in ranking_configs:
            train_result = run_underlying_rotation_backtest(
                dataset,
                config=config,
                start_date=train_start,
                end_date=train_end,
                starting_equity=starting_equity,
                risk_free_rate=risk_free_rate,
                ranking_plan=plan,
            )
            train_rows.append(
                UnderlyingRotationOptimizationRow(
                    config=config,
                    train_result=train_result,
                )
            )

    train_rows.sort(
        key=lambda row: _optimization_summary_sort_key(
            row.train_result.summary,
            objective=objective,
            max_drawdown_pct_cap=max_drawdown_pct_cap,
        ),
        reverse=True,
    )
    selected_rows = train_rows[: max(1, top_validation_count)]
    validation_cache: dict[tuple[tuple[int, int, int], tuple[float, float, float], int], _RankingPlan] = {}
    validated_rows: list[UnderlyingRotationOptimizationRow] = []
    for row in selected_rows:
        ranking_key = row.config.ranking_key
        plan = validation_cache.get(ranking_key)
        if plan is None:
            lookbacks, weights, rebalance_frequency = ranking_key
            plan = _build_ranking_plan(
                dataset,
                start_date=validation_start,
                end_date=validation_end,
                lookback_days=lookbacks,
                lookback_weights=weights,
                rebalance_frequency_days=rebalance_frequency,
                max_ranked_symbols=max_ranked_symbols,
            )
            validation_cache[ranking_key] = plan
        validation_result = run_underlying_rotation_backtest(
            dataset,
            config=row.config,
            start_date=validation_start,
            end_date=validation_end,
            starting_equity=starting_equity,
            risk_free_rate=risk_free_rate,
            ranking_plan=plan,
        )
        validated_rows.append(
            UnderlyingRotationOptimizationRow(
                config=row.config,
                train_result=row.train_result,
                validation_result=validation_result,
            )
        )

    if validated_rows:
        validated_rows.sort(
            key=lambda row: _optimization_summary_sort_key(
                row.validation_result.summary if row.validation_result is not None else row.train_result.summary,
                objective=objective,
                max_drawdown_pct_cap=max_drawdown_pct_cap,
            ),
            reverse=True,
        )
    best_row = validated_rows[0] if validated_rows else train_rows[0]
    return UnderlyingRotationOptimizationResult(
        universe_size=len(dataset.histories),
        candidate_count=len(configs),
        top_rows=tuple(validated_rows if validated_rows else train_rows[: max(1, top_validation_count)]),
        best_config=best_row.config,
        best_train_result=best_row.train_result,
        best_validation_result=best_row.validation_result,
    )


def build_rolling_split_result(
    split: UnderlyingRotationWalkForwardSplit,
    result: UnderlyingRotationOptimizationResult,
) -> UnderlyingRotationRollingSplitResult:
    return UnderlyingRotationRollingSplitResult(
        split=split,
        universe_size=result.universe_size,
        best_row=result.top_rows[0],
        all_rows=result.top_rows,
    )


def aggregate_rolling_walk_forward_results(
    split_results: tuple[UnderlyingRotationRollingSplitResult, ...],
    *,
    max_drawdown_pct_cap: float | None = None,
) -> tuple[UnderlyingRotationRollingConfigStats, ...]:
    metrics_by_config: dict[UnderlyingRotationConfig, list[dict[str, float | int | None]]] = defaultdict(list)
    for split_result in split_results:
        for row in split_result.all_rows:
            validation = row.validation_result.summary if row.validation_result is not None else None
            train = row.train_result.summary
            metrics_by_config[row.config].append(
                {
                    "train_year": split_result.split.train_start.year,
                    "validation_year": split_result.split.validation_start.year,
                    "train_roi_pct": train.total_roi_pct,
                    "train_max_drawdown_pct": train.max_drawdown_pct,
                    "train_sharpe": train.sharpe_ratio,
                    "validation_roi_pct": validation.total_roi_pct if validation is not None else None,
                    "validation_max_drawdown_pct": validation.max_drawdown_pct if validation is not None else None,
                    "validation_sharpe": validation.sharpe_ratio if validation is not None else None,
                }
            )

    expected_split_count = len(split_results)
    aggregated: list[UnderlyingRotationRollingConfigStats] = []
    for config, split_metrics in metrics_by_config.items():
        if len(split_metrics) != expected_split_count:
            continue
        validation_rois = [float(item["validation_roi_pct"]) for item in split_metrics if item["validation_roi_pct"] is not None]
        validation_dds = [float(item["validation_max_drawdown_pct"]) for item in split_metrics if item["validation_max_drawdown_pct"] is not None]
        validation_sharpes = [float(item["validation_sharpe"]) for item in split_metrics if item["validation_sharpe"] is not None]
        if not validation_rois or not validation_dds:
            continue
        aggregated.append(
            UnderlyingRotationRollingConfigStats(
                config=config,
                median_validation_roi_pct=_median(validation_rois),
                average_validation_roi_pct=(sum(validation_rois) / len(validation_rois)),
                median_validation_max_drawdown_pct=_median(validation_dds),
                average_validation_sharpe=(sum(validation_sharpes) / len(validation_sharpes)) if validation_sharpes else None,
                positive_validation_split_count=sum(1 for value in validation_rois if value > 0),
                within_drawdown_cap_split_count=sum(
                    1
                    for value in validation_dds
                    if max_drawdown_pct_cap is None or value <= max_drawdown_pct_cap
                ),
                split_count=expected_split_count,
                split_metrics=tuple(split_metrics),
            )
        )
    aggregated.sort(key=_rolling_config_sort_key, reverse=True)
    return tuple(aggregated)


def recommend_rolling_challenger(
    aggregated_stats: tuple[UnderlyingRotationRollingConfigStats, ...],
    *,
    incumbent_config: UnderlyingRotationConfig | None = None,
    min_median_validation_roi_improvement_pct: float = 0.0,
    require_non_worse_within_cap_count: bool = True,
) -> UnderlyingRotationRollingDecision:
    if not aggregated_stats:
        raise ValueError("aggregated_stats cannot be empty")
    challenger = aggregated_stats[0]
    if incumbent_config is None:
        return UnderlyingRotationRollingDecision(
            action="adopt_challenger",
            challenger=challenger,
            incumbent=None,
            median_validation_roi_improvement_pct=None,
            reason="No incumbent config was provided.",
        )

    incumbent = next((item for item in aggregated_stats if item.config == incumbent_config), None)
    if incumbent is None:
        return UnderlyingRotationRollingDecision(
            action="adopt_challenger",
            challenger=challenger,
            incumbent=None,
            median_validation_roi_improvement_pct=None,
            reason="Incumbent config is not present in the evaluated search space.",
        )

    roi_improvement = challenger.median_validation_roi_pct - incumbent.median_validation_roi_pct
    if challenger.config == incumbent.config:
        return UnderlyingRotationRollingDecision(
            action="keep_incumbent",
            challenger=challenger,
            incumbent=incumbent,
            median_validation_roi_improvement_pct=0.0,
            reason="Incumbent already matches the top-ranked challenger.",
        )
    if roi_improvement < min_median_validation_roi_improvement_pct:
        return UnderlyingRotationRollingDecision(
            action="keep_incumbent",
            challenger=challenger,
            incumbent=incumbent,
            median_validation_roi_improvement_pct=roi_improvement,
            reason="Median validation ROI improvement does not clear the replacement threshold.",
        )
    if require_non_worse_within_cap_count and (
        challenger.within_drawdown_cap_split_count < incumbent.within_drawdown_cap_split_count
    ):
        return UnderlyingRotationRollingDecision(
            action="keep_incumbent",
            challenger=challenger,
            incumbent=incumbent,
            median_validation_roi_improvement_pct=roi_improvement,
            reason="Challenger is worse on drawdown-cap consistency than the incumbent.",
        )
    return UnderlyingRotationRollingDecision(
        action="adopt_challenger",
        challenger=challenger,
        incumbent=incumbent,
        median_validation_roi_improvement_pct=roi_improvement,
        reason="Challenger clears the replacement threshold and is not worse on drawdown-cap consistency.",
    )


def run_underlying_rotation_backtest(
    dataset: UnderlyingRotationDataset,
    *,
    config: UnderlyingRotationConfig,
    start_date: date,
    end_date: date,
    starting_equity: float = 100_000.0,
    risk_free_rate: float = 0.0,
    ranking_plan: _RankingPlan | None = None,
) -> UnderlyingRotationBacktestResult:
    trade_dates = [item for item in dataset.trade_dates if start_date <= item <= end_date]
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
        return UnderlyingRotationBacktestResult(
            config=config,
            summary=summary,
            trades=(),
            equity_curve=(),
            warnings=tuple(warnings),
        )

    plan = ranking_plan or _build_ranking_plan(
        dataset,
        start_date=start_date,
        end_date=end_date,
        lookback_days=config.lookback_days,
        lookback_weights=config.lookback_weights,
        rebalance_frequency_days=config.rebalance_frequency_days,
        max_ranked_symbols=max(50, config.portfolio_size * 3),
    )
    cash = starting_equity
    open_positions: dict[str, _OpenPosition] = {}
    trades: list[TradeResult] = []
    equity_curve: list[EquityPointResult] = []
    running_peak = starting_equity
    for trade_date in trade_dates:
        target_symbols = plan.ranked_symbols_by_execution_date.get(trade_date)
        if target_symbols is not None:
            selected_symbols = _select_tradeable_symbols(dataset, trade_date, target_symbols, config.portfolio_size)
            selected_symbol_set = set(selected_symbols)
            for symbol in tuple(open_positions):
                if symbol in selected_symbol_set:
                    continue
                history = dataset.histories[symbol]
                bar = history.bar_on(trade_date)
                exit_price = bar.open_price if bar is not None else open_positions[symbol].last_close
                cash += _close_position(
                    open_positions.pop(symbol),
                    exit_date=trade_date,
                    exit_price=exit_price,
                    exit_reason="rebalance",
                    trades=trades,
                )
            new_symbols = tuple(symbol for symbol in selected_symbols if symbol not in open_positions)
            if new_symbols and cash > 0:
                allocation_per_symbol = cash / len(new_symbols)
                remaining_cash = cash
                for symbol in new_symbols:
                    bar = dataset.histories[symbol].bar_on(trade_date)
                    if bar is None or bar.open_price <= 0:
                        continue
                    share_quantity = allocation_per_symbol / bar.open_price
                    if share_quantity <= 0:
                        continue
                    invested_amount = share_quantity * bar.open_price
                    remaining_cash -= invested_amount
                    open_positions[symbol] = _OpenPosition(
                        symbol=symbol,
                        entry_date=trade_date,
                        entry_price=bar.open_price,
                        share_quantity=share_quantity,
                        highest_close=bar.open_price,
                        last_close=bar.open_price,
                    )
                cash = remaining_cash

        if config.trailing_stop_pct > 0 and open_positions:
            for symbol in tuple(open_positions):
                position = open_positions[symbol]
                bar = dataset.histories[symbol].bar_on(trade_date)
                if bar is None:
                    cash += _close_position(
                        open_positions.pop(symbol),
                        exit_date=trade_date,
                        exit_price=position.last_close,
                        exit_reason="data_unavailable",
                        trades=trades,
                    )
                    continue
                stop_price = position.highest_close * (1.0 - config.trailing_stop_pct)
                exit_price = _trailing_stop_exit_price(stop_price, bar)
                if exit_price is None:
                    continue
                cash += _close_position(
                    open_positions.pop(symbol),
                    exit_date=trade_date,
                    exit_price=exit_price,
                    exit_reason="trailing_stop",
                    trades=trades,
                    stop_price=stop_price,
                )

        position_value = 0.0
        for position in open_positions.values():
            bar = dataset.histories[position.symbol].bar_on(trade_date)
            if bar is None:
                position_value += position.share_quantity * position.last_close
                continue
            position.last_close = bar.close_price
            position.highest_close = max(position.highest_close, bar.close_price)
            position_value += position.share_quantity * bar.close_price

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
    for symbol in tuple(open_positions):
        position = open_positions.pop(symbol)
        bar = dataset.histories[symbol].bar_on(final_trade_date)
        exit_price = bar.close_price if bar is not None else position.last_close
        cash += _close_position(
            position,
            exit_date=final_trade_date,
            exit_price=exit_price,
            exit_reason="backtest_end",
            trades=trades,
        )

    summary = build_summary(
        starting_equity,
        float(equity_curve[-1].equity) if equity_curve else starting_equity,
        trades,
        equity_curve,
        risk_free_rate=risk_free_rate,
        warnings=warnings,
    )
    if not trades:
        warnings.append(
            {
                "code": "no_trades",
                "message": "No underlying rotation trades were generated for this parameter set.",
            }
        )
    return UnderlyingRotationBacktestResult(
        config=config,
        summary=summary,
        trades=tuple(trades),
        equity_curve=tuple(equity_curve),
        warnings=tuple(warnings),
    )


def _build_ranking_plan(
    dataset: UnderlyingRotationDataset,
    *,
    start_date: date,
    end_date: date,
    lookback_days: tuple[int, int, int],
    lookback_weights: tuple[float, float, float],
    rebalance_frequency_days: int,
    max_ranked_symbols: int,
) -> _RankingPlan:
    trade_dates = [item for item in dataset.trade_dates if start_date <= item <= end_date]
    ranked_symbols_by_execution_date: dict[date, tuple[str, ...]] = {}
    if len(trade_dates) < 2:
        return _RankingPlan(ranked_symbols_by_execution_date)

    for signal_index in range(0, len(trade_dates) - 1, rebalance_frequency_days):
        signal_date = trade_dates[signal_index]
        execution_date = trade_dates[signal_index + 1]
        scored_symbols: list[tuple[float, str]] = []
        for symbol, history in dataset.histories.items():
            signal_bar = history.bar_on(signal_date)
            if signal_bar is None or signal_bar.close_price <= 0:
                continue
            score = _score_symbol(
                history,
                signal_date=signal_date,
                current_close=signal_bar.close_price,
                lookback_days=lookback_days,
                lookback_weights=lookback_weights,
            )
            if score is None:
                continue
            scored_symbols.append((score, symbol))
        scored_symbols.sort(key=lambda item: (-item[0], item[1]))
        ranked_symbols_by_execution_date[execution_date] = tuple(
            symbol for _score, symbol in scored_symbols[:max_ranked_symbols]
        )
    return _RankingPlan(ranked_symbols_by_execution_date)


def _score_symbol(
    history: _PriceHistory,
    *,
    signal_date: date,
    current_close: float,
    lookback_days: tuple[int, int, int],
    lookback_weights: tuple[float, float, float],
) -> float | None:
    weighted_score = 0.0
    for lookback_day, weight in zip(lookback_days, lookback_weights, strict=True):
        base_close = history.close_on_or_before(signal_date - timedelta(days=lookback_day))
        if base_close is None or base_close <= 0:
            return None
        weighted_score += ((current_close / base_close) - 1.0) * weight
    return weighted_score


def _select_tradeable_symbols(
    dataset: UnderlyingRotationDataset,
    trade_date: date,
    ranked_symbols: tuple[str, ...],
    portfolio_size: int,
) -> tuple[str, ...]:
    selected: list[str] = []
    for symbol in ranked_symbols:
        history = dataset.histories.get(symbol)
        if history is None:
            continue
        bar = history.bar_on(trade_date)
        if bar is None or bar.open_price <= 0:
            continue
        selected.append(symbol)
        if len(selected) >= portfolio_size:
            break
    return tuple(selected)


def _trailing_stop_exit_price(stop_price: float, bar: DailyBar) -> float | None:
    if stop_price <= 0:
        return None
    if bar.open_price <= stop_price:
        return bar.open_price
    if bar.low_price <= stop_price:
        return stop_price
    return None


def _close_position(
    position: _OpenPosition,
    *,
    exit_date: date,
    exit_price: float,
    exit_reason: str,
    trades: list[TradeResult],
    stop_price: float | None = None,
) -> float:
    gross_pnl = (exit_price - position.entry_price) * position.share_quantity
    trade = TradeResult(
        option_ticker=position.symbol,
        strategy_type="underlying_rotation",
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
        entry_reason="rebalance",
        exit_reason=exit_reason,
        detail_json={
            "share_quantity": position.share_quantity,
            "entry_value": position.share_quantity * position.entry_price,
            "exit_value": position.share_quantity * exit_price,
            "trailing_stop_price": stop_price,
        },
    )
    trades.append(trade)
    return position.share_quantity * exit_price


def _summary_sort_key(summary: BacktestSummary) -> tuple[float, float, float]:
    sharpe = summary.sharpe_ratio if summary.sharpe_ratio is not None else float("-inf")
    return (sharpe, summary.total_roi_pct, -summary.max_drawdown_pct)


def _optimization_summary_sort_key(
    summary: BacktestSummary,
    *,
    objective: str,
    max_drawdown_pct_cap: float | None,
) -> tuple[float, float, float, float]:
    within_drawdown_cap = (
        1.0 if max_drawdown_pct_cap is None or summary.max_drawdown_pct <= max_drawdown_pct_cap else 0.0
    )
    sharpe = summary.sharpe_ratio if summary.sharpe_ratio is not None else float("-inf")
    if objective == "roi":
        return (within_drawdown_cap, summary.total_roi_pct, sharpe, -summary.max_drawdown_pct)
    return (within_drawdown_cap, sharpe, summary.total_roi_pct, -summary.max_drawdown_pct)


def _rolling_config_sort_key(stats: UnderlyingRotationRollingConfigStats) -> tuple[int, float, int, float]:
    return (
        stats.within_drawdown_cap_split_count,
        stats.median_validation_roi_pct,
        stats.positive_validation_split_count,
        -stats.median_validation_max_drawdown_pct,
    )


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0
