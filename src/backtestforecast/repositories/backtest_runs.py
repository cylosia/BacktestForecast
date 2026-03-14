from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session, noload, selectinload

from backtestforecast.models import BacktestEquityPoint, BacktestRun, BacktestTrade


class BacktestRunRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, run: BacktestRun) -> BacktestRun:
        self.session.add(run)
        self.session.flush()
        return run

    def get_by_id(self, run_id: UUID, *, for_update: bool = False) -> BacktestRun | None:
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
        stmt = select(BacktestRun).where(
            BacktestRun.user_id == user_id,
            BacktestRun.idempotency_key == idempotency_key,
        )
        return self.session.scalar(stmt)

    def list_for_user(
        self,
        user_id: UUID,
        *,
        limit: int = 50,
        offset: int = 0,
        created_since: datetime | None = None,
    ) -> list[BacktestRun]:
        stmt = (
            select(BacktestRun)
            .where(BacktestRun.user_id == user_id)
            .options(
                noload(BacktestRun.trades),
                noload(BacktestRun.equity_points),
            )
        )
        if created_since is not None:
            stmt = stmt.where(BacktestRun.created_at >= created_since)
        stmt = stmt.order_by(desc(BacktestRun.created_at)).offset(offset).limit(limit)
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
    ) -> int:
        stmt = select(func.count(BacktestRun.id)).where(
            BacktestRun.user_id == user_id,
            BacktestRun.created_at >= start_inclusive,
            BacktestRun.created_at < end_exclusive,
            BacktestRun.status.notin_(("failed", "cancelled")),
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
        stmt = (
            select(BacktestRun)
            .where(BacktestRun.id.in_(run_ids), BacktestRun.user_id == user_id)
        )
        return list(self.session.scalars(stmt))

    def get_trades_for_run(self, run_id: UUID, *, limit: int = 10_000, user_id: UUID | None = None) -> list[BacktestTrade]:
        stmt = (
            select(BacktestTrade)
            .where(BacktestTrade.run_id == run_id)
            .order_by(BacktestTrade.entry_date)
            .limit(limit)
        )
        if user_id is not None:
            stmt = stmt.join(BacktestRun, BacktestTrade.run_id == BacktestRun.id).where(BacktestRun.user_id == user_id)
        return list(self.session.scalars(stmt))

    def get_equity_points_for_run(self, run_id: UUID, *, limit: int = 10_000, user_id: UUID | None = None) -> list[BacktestEquityPoint]:
        stmt = (
            select(BacktestEquityPoint)
            .where(BacktestEquityPoint.run_id == run_id)
            .order_by(BacktestEquityPoint.trade_date)
            .limit(limit)
        )
        if user_id is not None:
            stmt = stmt.join(BacktestRun, BacktestEquityPoint.run_id == BacktestRun.id).where(BacktestRun.user_id == user_id)
        return list(self.session.scalars(stmt))
