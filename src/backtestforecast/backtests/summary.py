from __future__ import annotations

from statistics import fmean

from backtestforecast.backtests.types import BacktestSummary, EquityPointResult, TradeResult


def build_summary(
    starting_equity: float,
    ending_equity: float,
    trades: list[TradeResult],
    equity_curve: list[EquityPointResult],
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
    decided = len(win_pnls) + len(loss_pnls)
    win_rate = (len(win_pnls) / decided * 100.0) if decided else 0.0
    max_drawdown_pct = max((point.drawdown_pct for point in equity_curve), default=0.0)

    return BacktestSummary(
        trade_count=trade_count,
        win_rate=win_rate,
        total_roi_pct=((ending_equity - starting_equity) / starting_equity * 100.0) if starting_equity else 0.0,
        average_win_amount=fmean(win_pnls) if win_pnls else 0.0,
        average_loss_amount=fmean(loss_pnls) if loss_pnls else 0.0,
        average_holding_period_days=(sum_holding / trade_count) if trade_count else 0.0,
        average_dte_at_open=(sum_dte / trade_count) if trade_count else 0.0,
        max_drawdown_pct=max_drawdown_pct,
        total_commissions=total_commissions,
        total_net_pnl=total_net_pnl,
        starting_equity=starting_equity,
        ending_equity=ending_equity,
    )
