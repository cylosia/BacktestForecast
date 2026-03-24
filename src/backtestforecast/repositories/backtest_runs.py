from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import structlog
from sqlalchemy import case, func, select
from sqlalchemy.orm import Session, defer, noload, selectinload

from backtestforecast.models import BacktestEquityPoint, BacktestRun, BacktestTrade
from backtestforecast.repositories.pagination import apply_cursor_window, list_with_total

logger = structlog.get_logger("repositories.backtest_runs")

_MAX_PAGE_SIZE = 200


@dataclass(slots=True)
class BacktestRunTradeBatch:
    trades: list[BacktestTrade]
    total_count: int = 0

    @property
    def exceeded_limit(self) -> bool:
        return self.total_count > len(self.trades)


@dataclass(slots=True)
class BacktestRunPayloadCounts:
    trade_count: int = 0
    decided_trade_count: int = 0
    equity_point_count: int = 0


class BacktestRunRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, run: BacktestRun) -> BacktestRun:
        self.session.add(run)
        self.session.flush()
        return run

    def get_by_id_unfiltered(
        self,
        run_id: UUID,
        *,
        for_update: bool = False,
        load_relationships: bool = False,
    ) -> BacktestRun | None:
        """Fetch by PK without ownership filter. WORKER-ONLY - never call from API routes.

        Set *load_relationships* to ``True`` to eagerly load trades and
        equity points. Default is ``False`` to avoid pulling thousands of
        rows when only scalar columns are needed (e.g. after execution).
        """
        stmt = select(BacktestRun).where(BacktestRun.id == run_id)
        if for_update:
            stmt = stmt.with_for_update()
        elif load_relationships:
            stmt = stmt.options(
                selectinload(BacktestRun.trades),
                selectinload(BacktestRun.equity_points),
            )
        return self.session.scalar(stmt)

    def get_by_idempotency_key(self, user_id: UUID, idempotency_key: str) -> BacktestRun | None:
        stmt = (
            select(BacktestRun)
            .where(
                BacktestRun.user_id == user_id,
                BacktestRun.idempotency_key == idempotency_key,
                BacktestRun.status.notin_(["failed", "cancelled"]),
            )
            .with_for_update(skip_locked=False)
        )
        return self.session.scalar(stmt)

    def list_for_user(
        self,
        user_id: UUID,
        *,
        limit: int = 50,
        offset: int = 0,
        created_since: datetime | None = None,
        cursor_before: tuple[datetime, UUID] | None = None,
    ) -> list[BacktestRun]:
        stmt = (
            select(BacktestRun)
            .where(BacktestRun.user_id == user_id)
            .options(
                noload(BacktestRun.trades),
                noload(BacktestRun.equity_points),
                defer(BacktestRun.input_snapshot_json),
            )
        )
        if created_since is not None:
            stmt = stmt.where(BacktestRun.created_at >= created_since)
        stmt = apply_cursor_window(
            stmt,
            model=BacktestRun,
            cursor_before=cursor_before,
            limit=limit,
            offset=offset,
            max_page_size=_MAX_PAGE_SIZE,
        )
        return list(self.session.scalars(stmt))

    def count_for_user(
        self,
        user_id: UUID,
        *,
        created_since: datetime | None = None,
    ) -> int:
        stmt = select(func.count(BacktestRun.id)).where(BacktestRun.user_id == user_id)
        if created_since is not None:
            stmt = stmt.where(BacktestRun.created_at >= created_since)
        return int(self.session.scalar(stmt) or 0)

    def list_for_user_with_count(
        self,
        user_id: UUID,
        *,
        limit: int = 50,
        offset: int = 0,
        created_since: datetime | None = None,
        cursor_before: tuple[datetime, UUID] | None = None,
    ) -> tuple[list[BacktestRun], int]:
        """Return (runs, total_count) where total_count ignores cursor pagination."""
        base_filter = [BacktestRun.user_id == user_id]
        if created_since is not None:
            base_filter.append(BacktestRun.created_at >= created_since)
        stmt = (
            select(BacktestRun)
            .where(*base_filter)
            .options(
                noload(BacktestRun.trades),
                noload(BacktestRun.equity_points),
                defer(BacktestRun.input_snapshot_json),
            )
        )
        return list_with_total(
            self.session,
            base_stmt=stmt,
            count_stmt=select(func.count(BacktestRun.id)).where(*base_filter),
            model=BacktestRun,
            cursor_before=cursor_before,
            limit=limit,
            offset=offset,
            max_page_size=_MAX_PAGE_SIZE,
        )

    def count_for_user_created_between(
        self,
        user_id: UUID,
        *,
        start_inclusive: datetime,
        end_exclusive: datetime,
        exclude_error_codes: tuple[str, ...] = ("enqueue_failed",),
        exclude_id: UUID | None = None,
    ) -> int:
        stmt = select(func.count(BacktestRun.id)).where(
            BacktestRun.user_id == user_id,
            BacktestRun.created_at >= start_inclusive,
            BacktestRun.created_at < end_exclusive,
            BacktestRun.status.notin_(("failed", "cancelled")),
        )
        if exclude_error_codes:
            stmt = stmt.where(
                (BacktestRun.error_code.is_(None)) | (BacktestRun.error_code.notin_(exclude_error_codes))
            )
        if exclude_id is not None:
            stmt = stmt.where(BacktestRun.id != exclude_id)
        return int(self.session.scalar(stmt) or 0)

    def get_for_user(self, run_id: UUID, user_id: UUID) -> BacktestRun | None:
        """Full load including trades + equity curve (detail pages, exports)."""
        stmt = (
            select(BacktestRun)
            .where(BacktestRun.id == run_id, BacktestRun.user_id == user_id)
            .options(
                selectinload(BacktestRun.trades),
                selectinload(BacktestRun.equity_points),
            )
        )
        return self.session.scalar(stmt)

    def get_lightweight_for_user(self, run_id: UUID, user_id: UUID) -> BacktestRun | None:
        """Ownership check + scalar columns only; no collection eager-loading."""
        stmt = select(BacktestRun).where(BacktestRun.id == run_id, BacktestRun.user_id == user_id)
        return self.session.scalar(stmt)

    def get_many_for_user(self, run_ids: list[UUID], user_id: UUID) -> list[BacktestRun]:
        if not run_ids:
            return []
        if len(run_ids) > 50:
            logger.warning(
                "backtest_runs.get_many_truncated",
                requested=len(run_ids),
                limit=50,
                user_id=str(user_id),
            )
        run_ids = run_ids[:50]
        stmt = (
            select(BacktestRun)
            .where(BacktestRun.id.in_(run_ids), BacktestRun.user_id == user_id)
            .options(noload(BacktestRun.trades), noload(BacktestRun.equity_points))
            .order_by(BacktestRun.created_at.desc())
        )
        return list(self.session.scalars(stmt))

    def get_trades_for_run(self, run_id: UUID, *, limit: int = 10_000, user_id: UUID) -> list[BacktestTrade]:
        stmt = (
            select(BacktestTrade)
            .join(BacktestRun, BacktestTrade.run_id == BacktestRun.id)
            .where(BacktestTrade.run_id == run_id, BacktestRun.user_id == user_id)
            .order_by(BacktestTrade.entry_date)
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

    def get_equity_points_for_run(self, run_id: UUID, *, limit: int = 10_000, user_id: UUID) -> list[BacktestEquityPoint]:
        stmt = (
            select(BacktestEquityPoint)
            .join(BacktestRun, BacktestEquityPoint.run_id == BacktestRun.id)
            .where(BacktestEquityPoint.run_id == run_id, BacktestRun.user_id == user_id)
            .order_by(BacktestEquityPoint.trade_date)
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

    def count_trades_for_run(self, run_id: UUID, *, user_id: UUID) -> int:
        stmt = (
            select(func.count(BacktestTrade.id))
            .join(BacktestRun, BacktestTrade.run_id == BacktestRun.id)
            .where(BacktestTrade.run_id == run_id, BacktestRun.user_id == user_id)
        )
        return int(self.session.scalar(stmt) or 0)

    def count_equity_points_for_run(self, run_id: UUID, *, user_id: UUID) -> int:
        stmt = (
            select(func.count(BacktestEquityPoint.id))
            .join(BacktestRun, BacktestEquityPoint.run_id == BacktestRun.id)
            .where(BacktestEquityPoint.run_id == run_id, BacktestRun.user_id == user_id)
        )
        return int(self.session.scalar(stmt) or 0)

    def count_decided_trades_for_run(self, run_id: UUID, *, user_id: UUID) -> int:
        stmt = (
            select(func.count(BacktestTrade.id))
            .join(BacktestRun, BacktestTrade.run_id == BacktestRun.id)
            .where(
                BacktestTrade.run_id == run_id,
                BacktestRun.user_id == user_id,
                BacktestTrade.net_pnl != 0,
            )
        )
        return int(self.session.scalar(stmt) or 0)

    def get_payload_counts_for_run(self, run_id: UUID, *, user_id: UUID) -> BacktestRunPayloadCounts:
        return self.get_payload_counts_for_runs([run_id], user_id=user_id).get(
            run_id,
            BacktestRunPayloadCounts(),
        )

    def count_trades_for_runs(self, run_ids: list[UUID], *, user_id: UUID) -> dict[UUID, int]:
        if not run_ids:
            return {}
        run_ids = run_ids[:50]
        stmt = (
            select(BacktestTrade.run_id, func.count(BacktestTrade.id))
            .join(BacktestRun, BacktestTrade.run_id == BacktestRun.id)
            .where(
                BacktestTrade.run_id.in_(run_ids),
                BacktestRun.user_id == user_id,
            )
            .group_by(BacktestTrade.run_id)
        )
        counts = {rid: 0 for rid in run_ids}
        for run_id, count in self.session.execute(stmt):
            counts[run_id] = int(count or 0)
        return counts

    def get_trades_for_runs(
        self, run_ids: list[UUID], *, limit_per_run: int = 10_000, user_id: UUID,
    ) -> dict[UUID, BacktestRunTradeBatch]:
        if not run_ids:
            return {}
        run_ids = run_ids[:50]
        from sqlalchemy import func as sa_func
        row_num = sa_func.row_number().over(
            partition_by=BacktestTrade.run_id,
            order_by=BacktestTrade.entry_date,
        ).label("rn")
        sub = (
            select(BacktestTrade.id, BacktestTrade.run_id, row_num)
            .join(BacktestRun, BacktestTrade.run_id == BacktestRun.id)
            .where(BacktestTrade.run_id.in_(run_ids), BacktestRun.user_id == user_id)
            .subquery()
        )
        stmt = (
            select(BacktestTrade)
            .join(sub, BacktestTrade.id == sub.c.id)
            .where(sub.c.rn <= limit_per_run)
            .order_by(BacktestTrade.run_id, BacktestTrade.entry_date)
        )
        total_counts = self.count_trades_for_runs(run_ids, user_id=user_id)
        result: dict[UUID, BacktestRunTradeBatch] = {
            rid: BacktestRunTradeBatch(trades=[], total_count=total_counts.get(rid, 0)) for rid in run_ids
        }
        for trade in self.session.scalars(stmt):
            batch = result[trade.run_id]
            batch.trades.append(trade)
        return result

    def get_equity_points_for_runs(
        self, run_ids: list[UUID], *, limit_per_run: int = 10_000, user_id: UUID,
    ) -> dict[UUID, list[BacktestEquityPoint]]:
        if not run_ids:
            return {}
        run_ids = run_ids[:50]
        from sqlalchemy import func as sa_func
        row_num = sa_func.row_number().over(
            partition_by=BacktestEquityPoint.run_id,
            order_by=BacktestEquityPoint.trade_date,
        ).label("rn")
        sub = (
            select(BacktestEquityPoint.id, row_num)
            .join(BacktestRun, BacktestEquityPoint.run_id == BacktestRun.id)
            .where(BacktestEquityPoint.run_id.in_(run_ids), BacktestRun.user_id == user_id)
            .subquery()
        )
        stmt = (
            select(BacktestEquityPoint)
            .join(sub, BacktestEquityPoint.id == sub.c.id)
            .where(sub.c.rn <= limit_per_run)
            .order_by(BacktestEquityPoint.run_id, BacktestEquityPoint.trade_date)
        )
        result: dict[UUID, list[BacktestEquityPoint]] = {rid: [] for rid in run_ids}
        for pt in self.session.scalars(stmt):
            result[pt.run_id].append(pt)
        return result

    def count_equity_points_for_runs(self, run_ids: list[UUID], *, user_id: UUID) -> dict[UUID, int]:
        if not run_ids:
            return {}
        run_ids = run_ids[:50]
        stmt = (
            select(BacktestEquityPoint.run_id, func.count(BacktestEquityPoint.id))
            .join(BacktestRun, BacktestEquityPoint.run_id == BacktestRun.id)
            .where(
                BacktestEquityPoint.run_id.in_(run_ids),
                BacktestRun.user_id == user_id,
            )
            .group_by(BacktestEquityPoint.run_id)
        )
        counts = {rid: 0 for rid in run_ids}
        for run_id, count in self.session.execute(stmt):
            counts[run_id] = int(count or 0)
        return counts

    def get_decided_trade_counts_for_runs(
        self, run_ids: list[UUID], *, user_id: UUID,
    ) -> dict[UUID, int]:
        if not run_ids:
            return {}
        run_ids = run_ids[:50]
        stmt = (
            select(BacktestTrade.run_id, func.count(BacktestTrade.id))
            .join(BacktestRun, BacktestTrade.run_id == BacktestRun.id)
            .where(
                BacktestTrade.run_id.in_(run_ids),
                BacktestRun.user_id == user_id,
                BacktestTrade.net_pnl != 0,
            )
            .group_by(BacktestTrade.run_id)
        )
        counts = {rid: 0 for rid in run_ids}
        for run_id, count in self.session.execute(stmt):
            counts[run_id] = int(count or 0)
        return counts

    def get_payload_counts_for_runs(
        self,
        run_ids: list[UUID],
        *,
        user_id: UUID,
    ) -> dict[UUID, BacktestRunPayloadCounts]:
        if not run_ids:
            return {}
        run_ids = run_ids[:50]
        trade_counts = (
            select(
                BacktestTrade.run_id.label("run_id"),
                func.count(BacktestTrade.id).label("trade_count"),
                func.sum(
                    case(
                        (BacktestTrade.net_pnl != 0, 1),
                        else_=0,
                    )
                ).label("decided_trade_count"),
            )
            .group_by(BacktestTrade.run_id)
            .subquery()
        )
        equity_counts = (
            select(
                BacktestEquityPoint.run_id.label("run_id"),
                func.count(BacktestEquityPoint.id).label("equity_point_count"),
            )
            .group_by(BacktestEquityPoint.run_id)
            .subquery()
        )
        stmt = (
            select(
                BacktestRun.id,
                func.coalesce(trade_counts.c.trade_count, 0),
                func.coalesce(trade_counts.c.decided_trade_count, 0),
                func.coalesce(equity_counts.c.equity_point_count, 0),
            )
            .outerjoin(trade_counts, trade_counts.c.run_id == BacktestRun.id)
            .outerjoin(equity_counts, equity_counts.c.run_id == BacktestRun.id)
            .where(
                BacktestRun.id.in_(run_ids),
                BacktestRun.user_id == user_id,
            )
        )
        counts = {rid: BacktestRunPayloadCounts() for rid in run_ids}
        for run_id, trade_count, decided_trade_count, equity_point_count in self.session.execute(stmt):
            counts[run_id] = BacktestRunPayloadCounts(
                trade_count=int(trade_count or 0),
                decided_trade_count=int(decided_trade_count or 0),
                equity_point_count=int(equity_point_count or 0),
            )
        return counts
