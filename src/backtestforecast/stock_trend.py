from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import BacktestSummary, EquityPointResult, TradeResult
from backtestforecast.market_data.types import DailyBar

_D0 = Decimal("0")


def _D(value: float | int) -> Decimal:
    return Decimal(str(value))


@dataclass(frozen=True, slots=True)
class StockTrendBacktestResult:
    strategy_name: str
    summary: BacktestSummary
    trades: tuple[TradeResult, ...]
    equity_curve: tuple[EquityPointResult, ...]
    warnings: tuple[dict[str, str], ...]


@dataclass(slots=True)
class _OpenStockPosition:
    entry_date: date
    entry_index: int
    entry_price: float
    share_quantity: float
    highest_close: float
    last_close: float
    entry_reason: str


def run_stock_condition_backtest(
    bars: Sequence[DailyBar],
    *,
    symbol: str,
    strategy_name: str,
    condition_series: Sequence[bool],
    entry_gate_series: Sequence[bool] | None = None,
    start_date: date,
    end_date: date,
    starting_equity: float = 100_000.0,
    risk_free_rate: float = 0.0,
    trailing_stop_pct: float = 0.0,
) -> StockTrendBacktestResult:
    ordered_bars = tuple(sorted(bars, key=lambda item: item.trade_date))
    warnings: list[dict[str, str]] = []
    if trailing_stop_pct < 0 or trailing_stop_pct >= 1:
        raise ValueError("trailing_stop_pct must be between 0 and 1")
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
                "message": f"No price history was supplied for {symbol}.",
            }
        )
        return StockTrendBacktestResult(
            strategy_name=strategy_name,
            summary=summary,
            trades=(),
            equity_curve=(),
            warnings=tuple(warnings),
        )
    if len(condition_series) != len(ordered_bars):
        raise ValueError("condition_series length must match bars length")
    if entry_gate_series is not None and len(entry_gate_series) != len(ordered_bars):
        raise ValueError("entry_gate_series length must match bars length")
    gate_series = tuple(entry_gate_series) if entry_gate_series is not None else (True,) * len(ordered_bars)

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
                "message": f"No bars were available for {symbol} between {start_date.isoformat()} and {end_date.isoformat()}.",
            }
        )
        return StockTrendBacktestResult(
            strategy_name=strategy_name,
            summary=summary,
            trades=(),
            equity_curve=(),
            warnings=tuple(warnings),
        )

    cash = starting_equity
    trades: list[TradeResult] = []
    equity_curve: list[EquityPointResult] = []
    open_position: _OpenStockPosition | None = None
    pending_entry = False
    pending_exit = False
    running_peak = starting_equity
    first_active_index = active_indexes[0]
    last_active_index = active_indexes[-1]
    prior_hold_condition = bool(condition_series[first_active_index - 1]) if first_active_index > 0 else False
    prior_entry_condition = (
        bool(condition_series[first_active_index - 1]) and bool(gate_series[first_active_index - 1])
        if first_active_index > 0
        else False
    )

    if prior_entry_condition:
        initial_bar = ordered_bars[first_active_index]
        if initial_bar.open_price > 0:
            share_quantity = cash / initial_bar.open_price
            open_position = _OpenStockPosition(
                entry_date=initial_bar.trade_date,
                entry_index=first_active_index,
                entry_price=initial_bar.open_price,
                share_quantity=share_quantity,
                highest_close=initial_bar.open_price,
                last_close=initial_bar.close_price,
                entry_reason="carry_in_condition",
            )
            cash = 0.0

    for index in active_indexes:
        bar = ordered_bars[index]
        entered_today = False
        if pending_exit and open_position is not None:
            cash += _close_position(
                symbol=symbol,
                strategy_name=strategy_name,
                position=open_position,
                exit_date=bar.trade_date,
                exit_index=index,
                exit_price=bar.open_price,
                exit_reason="condition_off",
                trades=trades,
            )
            open_position = None
            pending_exit = False

        if pending_entry and open_position is None and cash > 0 and bar.open_price > 0:
            share_quantity = cash / bar.open_price
            open_position = _OpenStockPosition(
                entry_date=bar.trade_date,
                entry_index=index,
                entry_price=bar.open_price,
                share_quantity=share_quantity,
                highest_close=bar.open_price,
                last_close=bar.close_price,
                entry_reason="condition_on",
            )
            cash = 0.0
            pending_entry = False
            entered_today = True

        if trailing_stop_pct > 0 and open_position is not None:
            stop_price = open_position.highest_close * (1.0 - trailing_stop_pct)
            exit_price = _trailing_stop_exit_price(stop_price, bar)
            if exit_price is not None:
                cash += _close_position(
                    symbol=symbol,
                    strategy_name=strategy_name,
                    position=open_position,
                    exit_date=bar.trade_date,
                    exit_index=index,
                    exit_price=exit_price,
                    exit_reason="trailing_stop",
                    trades=trades,
                    stop_price=stop_price,
                )
                open_position = None

        position_value = 0.0
        if open_position is not None:
            open_position.last_close = bar.close_price
            open_position.highest_close = max(open_position.highest_close, bar.close_price)
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

        current_hold_condition = bool(condition_series[index])
        current_entry_condition = current_hold_condition and bool(gate_series[index])
        if index == last_active_index:
            prior_hold_condition = current_hold_condition
            prior_entry_condition = current_entry_condition
            continue
        if open_position is None and (not entered_today) and current_entry_condition and not prior_entry_condition:
            pending_entry = True
        elif open_position is not None and (not current_hold_condition) and prior_hold_condition:
            pending_exit = True
        prior_hold_condition = current_hold_condition
        prior_entry_condition = current_entry_condition

    final_bar = ordered_bars[last_active_index]
    if open_position is not None:
        cash += _close_position(
            symbol=symbol,
            strategy_name=strategy_name,
            position=open_position,
            exit_date=final_bar.trade_date,
            exit_index=last_active_index,
            exit_price=final_bar.close_price,
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
                "message": "No stock trend trades were generated for this condition.",
            }
        )
    return StockTrendBacktestResult(
        strategy_name=strategy_name,
        summary=summary,
        trades=tuple(trades),
        equity_curve=tuple(equity_curve),
        warnings=tuple(warnings),
    )


def _close_position(
    *,
    symbol: str,
    strategy_name: str,
    position: _OpenStockPosition,
    exit_date: date,
    exit_index: int,
    exit_price: float,
    exit_reason: str,
    trades: list[TradeResult],
    stop_price: float | None = None,
) -> float:
    gross_pnl = (exit_price - position.entry_price) * position.share_quantity
    trades.append(
        TradeResult(
            option_ticker=symbol,
            strategy_type=strategy_name,
            underlying_symbol=symbol,
            entry_date=position.entry_date,
            exit_date=exit_date,
            expiration_date=exit_date,
            quantity=1,
            dte_at_open=0,
            holding_period_days=max((exit_date - position.entry_date).days, 0),
            holding_period_trading_days=max(exit_index - position.entry_index, 0),
            entry_underlying_close=_D(position.entry_price),
            exit_underlying_close=_D(exit_price),
            entry_mid=_D(position.entry_price / 100.0),
            exit_mid=_D(exit_price / 100.0),
            gross_pnl=_D(gross_pnl),
            net_pnl=_D(gross_pnl),
            total_commissions=_D0,
            entry_reason=position.entry_reason,
            exit_reason=exit_reason,
            detail_json={
                "share_quantity": position.share_quantity,
                "entry_value": position.share_quantity * position.entry_price,
                "exit_value": position.share_quantity * exit_price,
                "trailing_stop_price": stop_price,
            },
        )
    )
    return position.share_quantity * exit_price


def _trailing_stop_exit_price(stop_price: float, bar: DailyBar) -> float | None:
    if stop_price <= 0:
        return None
    if bar.open_price <= stop_price:
        return bar.open_price
    if bar.low_price <= stop_price:
        return stop_price
    return None
