"""Backfill financial metrics for historic BacktestRun rows.

Reads all succeeded runs where ``profit_factor IS NULL`` (pre-migration-0012
rows), recomputes the summary from persisted trades + equity curve, and writes
the missing metric columns.

Usage (one-time):
    python -m backtestforecast.management.backfill_metrics
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from decimal import Decimal

import structlog
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import EquityPointResult, TradeResult
from backtestforecast.db.session import create_session
from backtestforecast.models import BacktestRun

logger = structlog.get_logger("backfill_metrics")

BATCH_SIZE = 100


def _to_decimal(v: float | None) -> Decimal | None:
    if v is None:
        return None
    return Decimal(str(round(v, 4)))


def backfill() -> int:
    # Soft guard: warn if running during peak business hours (9-16 ET)
    now_utc = datetime.now(UTC)
    try:
        import zoneinfo
        et = zoneinfo.ZoneInfo("America/New_York")
        hour_et = now_utc.astimezone(et).hour
        if 9 <= hour_et < 17:
            logger.warning(
                "backfill.peak_hours",
                hour_et=hour_et,
                message="Running during peak business hours (9-16 ET). Consider running off-peak.",
            )
    except Exception:
        pass

    updated = 0
    with create_session() as session:
        stmt = (
            select(BacktestRun.id)
            .where(
                BacktestRun.status == "succeeded",
                BacktestRun.profit_factor.is_(None),
            )
            .limit(10_000)
        )
        run_ids = list(session.scalars(stmt))
        logger.info("backfill.found_runs", count=len(run_ids))

        for i in range(0, len(run_ids), BATCH_SIZE):
            batch = run_ids[i : i + BATCH_SIZE]
            for run_id in batch:
                try:
                    run = session.execute(
                        select(BacktestRun)
                        .where(BacktestRun.id == run_id)
                        .options(
                            selectinload(BacktestRun.trades),
                            selectinload(BacktestRun.equity_points),
                        )
                        .with_for_update()
                    ).scalar_one_or_none()
                    if run is None or run.profit_factor is not None:
                        continue

                    trades = [
                        TradeResult(
                            option_ticker=t.option_ticker,
                            strategy_type=t.strategy_type,
                            underlying_symbol=run.symbol,
                            entry_date=t.entry_date,
                            exit_date=t.exit_date,
                            expiration_date=t.expiration_date,
                            quantity=t.quantity,
                            dte_at_open=t.dte_at_open,
                            holding_period_days=t.holding_period_days,
                            entry_underlying_close=t.entry_underlying_close,
                            exit_underlying_close=t.exit_underlying_close,
                            entry_mid=t.entry_mid,
                            exit_mid=t.exit_mid,
                            gross_pnl=t.gross_pnl,
                            net_pnl=t.net_pnl,
                            total_commissions=t.total_commissions,
                            entry_reason=t.entry_reason,
                            exit_reason=t.exit_reason,
                            detail_json=t.detail_json or {},
                        )
                        for t in run.trades
                    ]
                    equity_curve = [
                        EquityPointResult(
                            trade_date=p.trade_date,
                            equity=p.equity,
                            cash=p.cash,
                            position_value=p.position_value,
                            drawdown_pct=p.drawdown_pct,
                        )
                        for p in run.equity_points
                    ]

                    summary = build_summary(
                        float(run.starting_equity),
                        float(run.ending_equity),
                        trades,
                        equity_curve,
                    )

                    pf = _to_decimal(summary.profit_factor)
                    if pf is None:
                        pf = Decimal("0")
                    run.profit_factor = pf
                    run.payoff_ratio = _to_decimal(summary.payoff_ratio)
                    run.expectancy = _to_decimal(summary.expectancy) or Decimal("0")
                    run.sharpe_ratio = _to_decimal(summary.sharpe_ratio)
                    run.sortino_ratio = _to_decimal(summary.sortino_ratio)
                    run.cagr_pct = _to_decimal(summary.cagr_pct)
                    run.calmar_ratio = _to_decimal(summary.calmar_ratio)
                    run.max_consecutive_wins = summary.max_consecutive_wins
                    run.max_consecutive_losses = summary.max_consecutive_losses
                    run.recovery_factor = _to_decimal(summary.recovery_factor)
                    updated += 1
                except Exception:
                    logger.exception("backfill.row_failed", run_id=str(run_id))
                    continue

            session.commit()
            logger.info("backfill.batch_committed", batch_start=i, updated_so_far=updated)

    return updated


if __name__ == "__main__":
    total = backfill()
    logger.info("backfill.complete", total_updated=total)
    sys.exit(0)
