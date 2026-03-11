from __future__ import annotations

from statistics import fmean

from backtestforecast.backtests.types import BacktestSummary, EquityPointResult, TradeResult


def build_summary(
    starting_equity: float,
    ending_equity: float,
    trades: list[TradeResult],
    equity_curve: list[EquityPointResult],
) -> BacktestSummary:
    winners = [trade.net_pnl for trade in trades if trade.net_pnl > 0]
    losers = [trade.net_pnl for trade in trades if trade.net_pnl < 0]
    trade_count = len(trades)
    win_rate = (len(winners) / trade_count * 100.0) if trade_count else 0.0
    total_net_pnl = sum(trade.net_pnl for trade in trades)
    total_commissions = sum(trade.total_commissions for trade in trades)
    max_drawdown_pct = max((point.drawdown_pct for point in equity_curve), default=0.0)

    return BacktestSummary(
        trade_count=trade_count,
        win_rate=win_rate,
        total_roi_pct=((ending_equity - starting_equity) / starting_equity * 100.0) if starting_equity else 0.0,
        average_win_amount=fmean(winners) if winners else 0.0,
        average_loss_amount=fmean(losers) if losers else 0.0,
        average_holding_period_days=fmean([trade.holding_period_days for trade in trades]) if trades else 0.0,
        average_dte_at_open=fmean([trade.dte_at_open for trade in trades]) if trades else 0.0,
        max_drawdown_pct=max_drawdown_pct,
        total_commissions=total_commissions,
        total_net_pnl=total_net_pnl,
        starting_equity=starting_equity,
        ending_equity=ending_equity,
    )
