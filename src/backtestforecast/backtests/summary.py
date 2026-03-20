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
    """Build a summary of backtest results from trade and equity data.

    TradeResult and EquityPointResult financial fields are Decimal.
    All values are converted to float at this statistics boundary since
    summary metrics use ``math.sqrt`` and other float-only functions.

    **Annualization conventions**

    - Sharpe/Sortino: annualized using ``sqrt(252)`` (trading days), since
      daily returns are computed from the equity curve which contains only
      trading days.
    - CAGR: annualized using ``365.25`` calendar days, since CAGR measures
      calendar-time growth rate regardless of market open days.

    These conventions are intentionally different and both correct for
    their respective metrics. Do not "unify" them without understanding
    why they differ.

    Break-even trades (net_pnl == 0) are excluded from both wins and losses.
    This can inflate win rate when many trades break even, as the effective
    sample size is reduced. Consider this when interpreting results.
    """
    win_pnls: list[float] = []
    loss_pnls: list[float] = []
    total_net_pnl = 0.0
    total_commissions = 0.0
    total_dividends_received = 0.0
    sum_holding = 0.0
    sum_dte = 0.0
    for trade in trades:
        pnl_f = float(trade.net_pnl)
        if pnl_f > 0:
            win_pnls.append(pnl_f)
        elif pnl_f < 0:
            loss_pnls.append(pnl_f)
        total_net_pnl += pnl_f
        total_commissions += float(trade.total_commissions)
        total_dividends_received += float(trade.detail_json.get("dividends_received", 0.0) or 0.0)
        sum_holding += trade.holding_period_days
        sum_dte += trade.dte_at_open

    trade_count = len(trades)
    decided = len(win_pnls) + len(loss_pnls)
    win_rate = (len(win_pnls) / decided * 100.0) if decided else 0.0
    max_drawdown_pct = max((float(point.drawdown_pct) for point in equity_curve), default=0.0)
    avg_win = fmean(win_pnls) if win_pnls else 0.0
    avg_loss = fmean(loss_pnls) if loss_pnls else 0.0

    gross_wins = sum(win_pnls)
    gross_losses = abs(sum(loss_pnls))
    profit_factor = (gross_wins / gross_losses) if gross_losses > 0 else None
    payoff_ratio = (abs(avg_win / avg_loss)) if (win_pnls and loss_pnls and avg_loss != 0) else None
    expectancy = (total_net_pnl / trade_count) if trade_count else 0.0

    break_even_count = trade_count - decided
    if break_even_count > 0 and decided > 0 and warnings is not None:
        be_pct = break_even_count / trade_count * 100
        if be_pct >= 20:
            warnings.append({
                "code": "high_break_even_rate",
                "message": (
                    f"{break_even_count} of {trade_count} trades ({be_pct:.0f}%) "
                    f"broke even (net P&L = $0). Win rate ({win_rate:.1f}%) is calculated "
                    f"from the {decided} trades with non-zero P&L only."
                ),
            })

    sharpe_ratio, sortino_ratio = _compute_sharpe_sortino(equity_curve, risk_free_rate, trade_count)
    if sharpe_ratio is None and warnings is not None and trade_count > 0:
        if trade_count < _MIN_TRADES_FOR_RATIOS:
            warnings.append({
                "code": "ratios_insufficient_trades",
                "message": f"Sharpe and Sortino ratios are not reported because the backtest has only {trade_count} trades (minimum {_MIN_TRADES_FOR_RATIOS} required).",
            })
        elif len(equity_curve) < _MIN_EQUITY_POINTS_FOR_RATIOS:
            warnings.append({
                "code": "ratios_insufficient_data",
                "message": f"Sharpe and Sortino ratios are not reported because the backtest has only {len(equity_curve)} equity data points (minimum {_MIN_EQUITY_POINTS_FOR_RATIOS} required).",
            })
        elif equity_curve and (equity_curve[-1].trade_date - equity_curve[0].trade_date).days < _MIN_CALENDAR_DAYS_FOR_RATIOS:
            warnings.append({
                "code": "ratios_insufficient_duration",
                "message": f"Sharpe and Sortino ratios are not reported because the backtest spans fewer than {_MIN_CALENDAR_DAYS_FOR_RATIOS} calendar days.",
            })
    cagr_pct = _compute_cagr(starting_equity, ending_equity, equity_curve, warnings=warnings)

    if cagr_pct is None and warnings is not None and equity_curve:
        calendar_days = (equity_curve[-1].trade_date - equity_curve[0].trade_date).days
        if len(equity_curve) < _MIN_TRADING_DAYS_FOR_CAGR:
            warnings.append({
                "code": "cagr_insufficient_data",
                "message": f"CAGR is not reported because the backtest has only {len(equity_curve)} trading day data points (minimum {_MIN_TRADING_DAYS_FOR_CAGR} required).",
            })
        elif calendar_days < 60:
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
        running_peak = starting_equity
        max_drawdown_dollars = 0.0
        for pt in equity_curve:
            eq_f = float(pt.equity)
            if eq_f > running_peak:
                running_peak = eq_f
            dd = running_peak - eq_f
            if dd > max_drawdown_dollars:
                max_drawdown_dollars = dd
        if max_drawdown_dollars > 0:
            recovery_factor = total_net_pnl / max_drawdown_dollars

    return BacktestSummary(
        trade_count=trade_count,
        decided_trades=decided,
        win_rate=win_rate,
        total_roi_pct=((ending_equity - starting_equity) / starting_equity * 100.0) if starting_equity > 0 else 0.0,
        average_win_amount=avg_win,
        average_loss_amount=avg_loss,
        average_holding_period_days=(sum_holding / trade_count) if trade_count else 0.0,
        average_dte_at_open=(sum_dte / trade_count) if trade_count else 0.0,
        max_drawdown_pct=max_drawdown_pct,
        total_commissions=total_commissions,
        total_net_pnl=total_net_pnl,
        total_dividends_received=total_dividends_received,
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
_MIN_EQUITY_POINTS_FOR_RATIOS = 20
_MIN_CALENDAR_DAYS_FOR_RATIOS = 30


def _compute_sharpe_sortino(
    equity_curve: list[EquityPointResult],
    risk_free_rate: float,
    trade_count: int,
) -> tuple[float | None, float | None]:
    """Compute annualised Sharpe and Sortino ratios.

    **Methodology notes (Sortino ratio)**

    The downside deviation is computed as::

        DD = sqrt( sum(min(r_i - rf, 0)^2 for all i) / (N - 1) )

    This differs from some textbook definitions in two ways:

    1. **N-1 denominator** (sample statistic) instead of N (population).
       We use N-1 for consistency with the Sharpe ratio so the two are
       directly comparable on the same data set.  The difference is
       negligible for backtests with many observations.

    2. **Only negative excess returns contribute** to the numerator sum,
       but the denominator uses the full observation count N-1.  This is
       the "partial downside deviation" variant (Sortino & Price 1994).
       Some implementations divide only by the count of negative returns,
       which inflates the ratio when few observations are negative.

    Values may differ from other platforms that use population statistics
    or alternative downside deviation formulas.
    """
    if trade_count < _MIN_TRADES_FOR_RATIOS or len(equity_curve) < 2:
        return None, None

    if len(equity_curve) < _MIN_EQUITY_POINTS_FOR_RATIOS:
        return None, None
    calendar_days = (equity_curve[-1].trade_date - equity_curve[0].trade_date).days
    if calendar_days < _MIN_CALENDAR_DAYS_FOR_RATIOS:
        return None, None

    equities = [float(point.equity) for point in equity_curve]
    first_nonpositive = next((idx for idx, eq in enumerate(equities) if eq <= 0), None)
    if first_nonpositive is not None:
        if first_nonpositive < 2:
            return None, None
        equity_curve = equity_curve[:first_nonpositive]
        equities = equities[:first_nonpositive]
        if len(equity_curve) < _MIN_EQUITY_POINTS_FOR_RATIOS:
            return None, None
        calendar_days = (equity_curve[-1].trade_date - equity_curve[0].trade_date).days
        if calendar_days < _MIN_CALENDAR_DAYS_FOR_RATIOS:
            return None, None
    daily_rf = risk_free_rate / 252.0
    excess: list[float] = []
    for i in range(1, len(equities)):
        daily_return = (equities[i] - equities[i - 1]) / equities[i - 1]
        excess.append(daily_return - daily_rf)

    if len(excess) < 2:
        return None, None

    n = len(excess)
    mean_excess = sum(excess) / n
    variance = sum((x - mean_excess) ** 2 for x in excess) / (n - 1)
    stddev = math.sqrt(variance) if variance > 0 else 0.0
    ann = math.sqrt(252.0)

    sharpe = (mean_excess / stddev * ann) if stddev > 0 else None

    downside_sq_sum = sum(x**2 for x in excess if x < 0)
    if downside_sq_sum > 0 and n > 1:
        down_dev = math.sqrt(downside_sq_sum / (n - 1))
        sortino = (mean_excess / down_dev * ann) if down_dev > 0 else None
    else:
        sortino = None

    return sharpe, sortino


def _compute_cagr(
    starting_equity: float,
    ending_equity: float,
    equity_curve: list[EquityPointResult],
    warnings: list[dict[str, str]] | None = None,
) -> float | None:
    if not equity_curve or len(equity_curve) < _MIN_TRADING_DAYS_FOR_CAGR:
        return None
    if starting_equity <= 0:
        return None
    if ending_equity <= 0:
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
    _CAGR_CAP = 10_000.0  # 10,000% annualized
    if abs(result) > _CAGR_CAP:
        if warnings is not None:
            warnings.append({
                "code": "cagr_capped",
                "message": (
                    f"CAGR was capped at {'+' if result > 0 else '-'}{_CAGR_CAP:.0f}% "
                    f"(raw: {result:,.1f}%). Short observation windows "
                    f"({calendar_days} days) produce extreme annualized returns "
                    f"that are not statistically meaningful."
                ),
            })
        result = _CAGR_CAP if result > 0 else -_CAGR_CAP
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
