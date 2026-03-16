from __future__ import annotations

import math
from statistics import fmean

from backtestforecast.backtests.types import BacktestSummary, EquityPointResult, TradeResult


def build_summary(
    starting_equity: float,
    ending_equity: float,
    trades: list[TradeResult],
    equity_curve: list[EquityPointResult],
    *,
    risk_free_rate: float = 0.045,
    warnings: list[dict[str, str]] | None = None,
) -> BacktestSummary:
    win_pnls: list[float] = []
    loss_pnls: list[float] = []
    total_net_pnl = 0.0
    total_commissions = 0.0
    sum_holding = 0.0
    sum_dte = 0.0
    for trade in trades:
        if trade.net_pnl > 0:
            win_pnls.append(trade.net_pnl)
        elif trade.net_pnl < 0:
            loss_pnls.append(trade.net_pnl)
        total_net_pnl += trade.net_pnl
        total_commissions += trade.total_commissions
        sum_holding += trade.holding_period_days
        sum_dte += trade.dte_at_open

    trade_count = len(trades)
    # Break-even trades (net_pnl == 0) are excluded from the win/loss
    # denominator so that win_rate reflects only decisive outcomes.
    decided = len(win_pnls) + len(loss_pnls)
    win_rate = (len(win_pnls) / decided * 100.0) if decided else 0.0
    max_drawdown_pct = max((point.drawdown_pct for point in equity_curve), default=0.0)
    avg_win = fmean(win_pnls) if win_pnls else 0.0
    avg_loss = fmean(loss_pnls) if loss_pnls else 0.0

    gross_wins = sum(win_pnls)
    gross_losses = abs(sum(loss_pnls))
    profit_factor = (gross_wins / gross_losses) if gross_losses > 0 else None
    payoff_ratio = (abs(avg_win / avg_loss)) if (win_pnls and loss_pnls and avg_loss != 0) else None
    expectancy = (total_net_pnl / trade_count) if trade_count else 0.0

    sharpe_ratio, sortino_ratio = _compute_sharpe_sortino(equity_curve, risk_free_rate, trade_count)
    cagr_pct = _compute_cagr(starting_equity, ending_equity, equity_curve)

    if cagr_pct is None and warnings is not None and equity_curve:
        calendar_days = (equity_curve[-1].trade_date - equity_curve[0].trade_date).days
        if calendar_days < 60:
            warnings.append({
                "code": "cagr_insufficient_duration",
                "message": f"CAGR is not reported because the backtest spans only {calendar_days} calendar days (minimum 60 required).",
            })

    calmar_ratio: float | None = None
    if cagr_pct is not None and max_drawdown_pct > 0:
        calmar_ratio = cagr_pct / max_drawdown_pct

    max_consecutive_wins, max_consecutive_losses = _compute_streaks(trades)

    recovery_factor: float | None = None
    if max_drawdown_pct > 0 and equity_curve:
        running_peak = equity_curve[0].equity
        max_drawdown_dollars = 0.0
        for pt in equity_curve:
            if pt.equity > running_peak:
                running_peak = pt.equity
            dd = running_peak - pt.equity
            if dd > max_drawdown_dollars:
                max_drawdown_dollars = dd
        if max_drawdown_dollars > 0:
            recovery_factor = total_net_pnl / max_drawdown_dollars
            if total_net_pnl < 0:
                recovery_factor = None

    return BacktestSummary(
        trade_count=trade_count,
        win_rate=win_rate,
        total_roi_pct=((ending_equity - starting_equity) / starting_equity * 100.0) if starting_equity else 0.0,
        average_win_amount=avg_win,
        average_loss_amount=avg_loss,
        average_holding_period_days=(sum_holding / trade_count) if trade_count else 0.0,
        average_dte_at_open=(sum_dte / trade_count) if trade_count else 0.0,
        max_drawdown_pct=max_drawdown_pct,
        total_commissions=total_commissions,
        total_net_pnl=total_net_pnl,
        starting_equity=starting_equity,
        ending_equity=ending_equity,
        profit_factor=profit_factor,
        payoff_ratio=payoff_ratio,
        expectancy=expectancy,
        sharpe_ratio=sharpe_ratio,
        sortino_ratio=sortino_ratio,
        cagr_pct=cagr_pct,
        calmar_ratio=calmar_ratio,
        max_consecutive_wins=max_consecutive_wins,
        max_consecutive_losses=max_consecutive_losses,
        recovery_factor=recovery_factor,
    )


_MIN_TRADES_FOR_RATIOS = 5
_MIN_TRADING_DAYS_FOR_CAGR = 10


def _compute_sharpe_sortino(
    equity_curve: list[EquityPointResult],
    risk_free_rate: float,
    trade_count: int,
) -> tuple[float | None, float | None]:
    if trade_count < _MIN_TRADES_FOR_RATIOS or len(equity_curve) < 2:
        return None, None

    equities = [point.equity for point in equity_curve]
    daily_rf = risk_free_rate / 252.0
    excess: list[float] = []
    for i in range(1, len(equities)):
        if equities[i - 1] <= 0:
            continue
        daily_return = (equities[i] - equities[i - 1]) / equities[i - 1]
        excess.append(daily_return - daily_rf)

    if len(excess) < 2:
        return None, None

    mean_excess = sum(excess) / len(excess)
    variance = sum((x - mean_excess) ** 2 for x in excess) / (len(excess) - 1)
    stddev = math.sqrt(variance) if variance > 0 else 0.0
    ann = math.sqrt(252.0)

    sharpe = (mean_excess / stddev * ann) if stddev > 0 else None

    downside_sq_sum = sum(x**2 for x in excess if x < 0)
    if downside_sq_sum > 0:
        down_dev = math.sqrt(downside_sq_sum / len(excess))
        sortino = (mean_excess / down_dev * ann) if down_dev > 0 else None
    else:
        sortino = None

    return sharpe, sortino


def _compute_cagr(
    starting_equity: float,
    ending_equity: float,
    equity_curve: list[EquityPointResult],
) -> float | None:
    if not equity_curve or len(equity_curve) < _MIN_TRADING_DAYS_FOR_CAGR:
        return None
    if starting_equity <= 0 or ending_equity <= 0:
        return None
    calendar_days = (equity_curve[-1].trade_date - equity_curve[0].trade_date).days
    # Require at least 60 calendar days to avoid misleadingly large annualised
    # returns from very short observation windows.  For example, a 10% return
    # over 30 days would annualise to ~244% CAGR which is not statistically
    # meaningful.
    if calendar_days < 60:
        return None
    ratio = ending_equity / starting_equity
    years = calendar_days / 365.25
    exponent = 1.0 / years
    result = (ratio**exponent - 1.0) * 100.0
    if not math.isfinite(result):
        return None
    return result


def _compute_streaks(trades: list[TradeResult]) -> tuple[int, int]:
    """Return (max_consecutive_wins, max_consecutive_losses).

    Break-even trades (net_pnl == 0) reset both streak counters.
    """
    max_w = max_l = cur_w = cur_l = 0
    for trade in trades:
        if trade.net_pnl > 0:
            cur_w += 1
            cur_l = 0
            max_w = max(max_w, cur_w)
        elif trade.net_pnl < 0:
            cur_l += 1
            cur_w = 0
            max_l = max(max_l, cur_l)
        else:
            cur_w = cur_l = 0
    return max_w, max_l
