from __future__ import annotations

from datetime import datetime
from uuid import UUID

import structlog
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session, defer, noload, selectinload

from backtestforecast.models import BacktestEquityPoint, BacktestRun, BacktestTrade

logger = structlog.get_logger("repositories.backtest_runs")

_MAX_PAGE_SIZE = 200


class BacktestRunRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, run: BacktestRun) -> BacktestRun:
        self.session.add(run)
        self.session.flush()
        return run

    def get_by_id_unfiltered(self, run_id: UUID, *, for_update: bool = False) -> BacktestRun | None:
        """Fetch by PK without ownership filter. WORKER-ONLY — never call from API routes."""
        stmt = select(BacktestRun).where(BacktestRun.id == run_id)
        if for_update:
            stmt = stmt.with_for_update()
        else:
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
        cursor_before: datetime | None = None,
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
        if cursor_before is not None:
            stmt = stmt.where(BacktestRun.created_at < cursor_before)
        stmt = stmt.order_by(desc(BacktestRun.created_at)).offset(offset).limit(min(limit, _MAX_PAGE_SIZE))
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

    def count_for_user_created_between(
        self,
        user_id: UUID,
        *,
        start_inclusive: datetime,
        end_exclusive: datetime,
        exclude_error_codes: tuple[str, ...] = ("enqueue_failed",),
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

    def get_status_for_user(self, run_id: UUID, user_id: UUID) -> BacktestRun | None:
        """Alias for lightweight lookup (polling endpoints)."""
        return self.get_lightweight_for_user(run_id, user_id)

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

    def get_trades_for_runs(
        self, run_ids: list[UUID], *, limit_per_run: int = 10_000, user_id: UUID,
    ) -> dict[UUID, list[BacktestTrade]]:
        if not run_ids:
            return {}
        from sqlalchemy import func as sa_func
        from sqlalchemy.orm import aliased
        row_num = sa_func.row_number().over(
            partition_by=BacktestTrade.run_id,
            order_by=BacktestTrade.entry_date,
        ).label("rn")
        sub = (
            select(BacktestTrade.id, row_num)
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
        result: dict[UUID, list[BacktestTrade]] = {rid: [] for rid in run_ids}
        for trade in self.session.scalars(stmt):
            result[trade.run_id].append(trade)
        return result

    def get_equity_points_for_runs(
        self, run_ids: list[UUID], *, limit_per_run: int = 10_000, user_id: UUID,
    ) -> dict[UUID, list[BacktestEquityPoint]]:
        if not run_ids:
            return {}
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
