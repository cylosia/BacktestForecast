from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import BacktestSummary, EquityPointResult, TradeResult
from backtestforecast.indicators.calculations import rsi
from backtestforecast.market_data.types import DailyBar

_D0 = Decimal("0")


def _D(value: float | int) -> Decimal:
    return Decimal(str(value))


@dataclass(frozen=True, slots=True)
class StockRsiConfig:
    symbol: str
    rsi_period: int = 14
    entry_level: float = 30.0
    exit_level: float = 70.0
    entry_direction: str = "crosses_above"
    exit_direction: str = "crosses_below"

    def __post_init__(self) -> None:
        if self.rsi_period < 1:
            raise ValueError("rsi_period must be >= 1")
        if self.entry_direction not in {"crosses_above", "crosses_below"}:
            raise ValueError("entry_direction must be 'crosses_above' or 'crosses_below'")
        if self.exit_direction not in {"crosses_above", "crosses_below"}:
            raise ValueError("exit_direction must be 'crosses_above' or 'crosses_below'")
        if not 0.0 <= self.entry_level <= 100.0:
            raise ValueError("entry_level must be between 0 and 100")
        if not 0.0 <= self.exit_level <= 100.0:
            raise ValueError("exit_level must be between 0 and 100")
        object.__setattr__(self, "symbol", self.symbol.upper())


@dataclass(frozen=True, slots=True)
class StockRsiBacktestResult:
    config: StockRsiConfig
    summary: BacktestSummary
    trades: tuple[TradeResult, ...]
    equity_curve: tuple[EquityPointResult, ...]
    warnings: tuple[dict[str, str], ...]
    rsi_series: tuple[float | None, ...]


@dataclass(slots=True)
class _OpenStockPosition:
    entry_date: date
    signal_date: date
    entry_price: float
    share_quantity: float
    last_close: float
    entry_signal_rsi: float


@dataclass(frozen=True, slots=True)
class _PendingSignal:
    signal_date: date
    signal_rsi: float


def run_stock_rsi_backtest(
    bars: Sequence[DailyBar],
    *,
    config: StockRsiConfig,
    start_date: date,
    end_date: date,
    starting_equity: float = 100_000.0,
    risk_free_rate: float = 0.0,
) -> StockRsiBacktestResult:
    ordered_bars = tuple(sorted(bars, key=lambda item: item.trade_date))
    warnings: list[dict[str, str]] = []
    if not ordered_bars:
        summary = build_summary(
            starting_equity,
            starting_equity,
            [],
            [],
            risk_free_rate=risk_free_rate,
            warnings=warnings,
        )
        warnings.append(
            {
                "code": "no_price_history",
                "message": f"No price history was supplied for {config.symbol}.",
            }
        )
        return StockRsiBacktestResult(
            config=config,
            summary=summary,
            trades=(),
            equity_curve=(),
            warnings=tuple(warnings),
            rsi_series=(),
        )

    closes = [bar.close_price for bar in ordered_bars]
    rsi_series = tuple(rsi(closes, config.rsi_period))
    active_indexes = [index for index, bar in enumerate(ordered_bars) if start_date <= bar.trade_date <= end_date]
    if not active_indexes:
        summary = build_summary(
            starting_equity,
            starting_equity,
            [],
            [],
            risk_free_rate=risk_free_rate,
            warnings=warnings,
        )
        warnings.append(
            {
                "code": "no_bars_in_range",
                "message": (
                    f"No bars were available for {config.symbol} between "
                    f"{start_date.isoformat()} and {end_date.isoformat()}."
                ),
            }
        )
        return StockRsiBacktestResult(
            config=config,
            summary=summary,
            trades=(),
            equity_curve=(),
            warnings=tuple(warnings),
            rsi_series=rsi_series,
        )

    cash = starting_equity
    open_position: _OpenStockPosition | None = None
    pending_entry: _PendingSignal | None = None
    pending_exit: _PendingSignal | None = None
    trades: list[TradeResult] = []
    equity_curve: list[EquityPointResult] = []
    running_peak = starting_equity
    last_active_index = active_indexes[-1]

    for index in active_indexes:
        bar = ordered_bars[index]
        if pending_exit is not None and open_position is not None:
            cash += _close_position(
                config=config,
                position=open_position,
                exit_date=bar.trade_date,
                exit_price=bar.open_price,
                exit_reason=config.exit_direction,
                exit_signal=pending_exit,
                trades=trades,
            )
            open_position = None
            pending_exit = None

        if pending_entry is not None and open_position is None and cash > 0 and bar.open_price > 0:
            share_quantity = cash / bar.open_price
            open_position = _OpenStockPosition(
                entry_date=bar.trade_date,
                signal_date=pending_entry.signal_date,
                entry_price=bar.open_price,
                share_quantity=share_quantity,
                last_close=bar.close_price,
                entry_signal_rsi=pending_entry.signal_rsi,
            )
            cash = 0.0
            pending_entry = None

        position_value = 0.0
        if open_position is not None:
            open_position.last_close = bar.close_price
            position_value = open_position.share_quantity * bar.close_price

        equity = cash + position_value
        running_peak = max(running_peak, equity)
        drawdown_pct = ((running_peak - equity) / running_peak * 100.0) if running_peak > 0 else 0.0
        equity_curve.append(
            EquityPointResult(
                trade_date=bar.trade_date,
                equity=_D(equity),
                cash=_D(cash),
                position_value=_D(position_value),
                drawdown_pct=_D(drawdown_pct),
            )
        )

        if index == last_active_index:
            continue

        current_rsi = rsi_series[index]
        previous_rsi = rsi_series[index - 1] if index > 0 else None
        if open_position is None:
            if _crossed(previous_rsi, current_rsi, level=config.entry_level, direction=config.entry_direction):
                pending_entry = _PendingSignal(
                    signal_date=bar.trade_date,
                    signal_rsi=float(current_rsi),
                )
        elif _crossed(previous_rsi, current_rsi, level=config.exit_level, direction=config.exit_direction):
            pending_exit = _PendingSignal(
                signal_date=bar.trade_date,
                signal_rsi=float(current_rsi),
            )

    final_bar = ordered_bars[last_active_index]
    if open_position is not None:
        cash += _close_position(
            config=config,
            position=open_position,
            exit_date=final_bar.trade_date,
            exit_price=final_bar.close_price,
            exit_reason="backtest_end",
            exit_signal=None,
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
                "message": "No RSI crossover trades were generated for this configuration.",
            }
        )
    return StockRsiBacktestResult(
        config=config,
        summary=summary,
        trades=tuple(trades),
        equity_curve=tuple(equity_curve),
        warnings=tuple(warnings),
        rsi_series=rsi_series,
    )


def _crossed(
    previous_value: float | None,
    current_value: float | None,
    *,
    level: float,
    direction: str,
) -> bool:
    if previous_value is None or current_value is None:
        return False
    if direction == "crosses_above":
        return previous_value <= level and current_value > level
    return previous_value >= level and current_value < level


def _close_position(
    *,
    config: StockRsiConfig,
    position: _OpenStockPosition,
    exit_date: date,
    exit_price: float,
    exit_reason: str,
    exit_signal: _PendingSignal | None,
    trades: list[TradeResult],
) -> float:
    gross_pnl = (exit_price - position.entry_price) * position.share_quantity
    trade = TradeResult(
        option_ticker=config.symbol,
        strategy_type="stock_rsi_long",
        underlying_symbol=config.symbol,
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
        entry_reason="rsi_entry",
        exit_reason=exit_reason,
        detail_json={
            "share_quantity": position.share_quantity,
            "entry_signal_date": position.signal_date.isoformat(),
            "entry_signal_rsi": position.entry_signal_rsi,
            "entry_level": config.entry_level,
            "entry_direction": config.entry_direction,
            "exit_signal_date": exit_signal.signal_date.isoformat() if exit_signal is not None else None,
            "exit_signal_rsi": exit_signal.signal_rsi if exit_signal is not None else None,
            "exit_level": config.exit_level,
            "exit_direction": config.exit_direction,
            "entry_value": position.share_quantity * position.entry_price,
            "exit_value": position.share_quantity * exit_price,
        },
    )
    trades.append(trade)
    return position.share_quantity * exit_price
