from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backtestforecast.billing.entitlements import resolve_feature_policy
from backtestforecast.errors import NotFoundError, QuotaExceededError
from backtestforecast.models import BacktestRun, MultiStepRun, MultiSymbolRun, User

UTC = UTC


def _current_month_window(*, now: datetime | None = None) -> tuple[datetime, datetime]:
    current = now or datetime.now(UTC)
    month_start = datetime(current.year, current.month, 1, tzinfo=UTC)
    next_month_start = (
        datetime(current.year + 1, 1, 1, tzinfo=UTC)
        if current.month == 12
        else datetime(current.year, current.month + 1, 1, tzinfo=UTC)
    )
    return month_start, next_month_start


def count_backtest_family_runs_for_current_month(
    session: Session,
    user_id: UUID,
    *,
    exclude_run_id: UUID | None = None,
    now: datetime | None = None,
) -> int:
    start_inclusive, end_exclusive = _current_month_window(now=now)

    def _count_stmt(model_cls):
        stmt = select(func.count(model_cls.id)).where(
            model_cls.user_id == user_id,
            model_cls.created_at >= start_inclusive,
            model_cls.created_at < end_exclusive,
            model_cls.status.notin_(("failed", "cancelled")),
        )
        if exclude_run_id is not None:
            stmt = stmt.where(model_cls.id != exclude_run_id)
        if model_cls is BacktestRun:
            stmt = stmt.where(
                (BacktestRun.error_code.is_(None)) | (BacktestRun.error_code.notin_(("enqueue_failed",)))
            )
        return stmt

    return int(
        (session.scalar(_count_stmt(BacktestRun)) or 0)
        + (session.scalar(_count_stmt(MultiSymbolRun)) or 0)
        + (session.scalar(_count_stmt(MultiStepRun)) or 0)
    )


def enforce_backtest_workflow_quota(
    session: Session,
    user: User,
    *,
    exclude_run_id: UUID | None = None,
) -> None:
    """Apply the same shared monthly quota across all backtest-family workflows."""
    locked_user = session.execute(
        select(User).where(User.id == user.id).with_for_update()
    ).scalar_one_or_none()
    if locked_user is None:
        raise NotFoundError("User not found.")

    policy = resolve_feature_policy(
        locked_user.plan_tier,
        locked_user.subscription_status,
        locked_user.subscription_current_period_end,
    )
    if policy.monthly_backtest_quota is None:
        return

    used_this_month = count_backtest_family_runs_for_current_month(
        session,
        locked_user.id,
        exclude_run_id=exclude_run_id,
    )
    if used_this_month >= policy.monthly_backtest_quota:
        raise QuotaExceededError(
            f"The {policy.tier.value} plan allows "
            f"{policy.monthly_backtest_quota} backtests per month. "
            f"You have used {used_this_month}. Upgrade your plan for more.",
            current_tier=policy.tier.value,
        )
