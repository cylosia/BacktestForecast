"""add async backtest run support

Revision ID: 20260310_0005
Revises: 20260310_0004
Create Date: 2026-03-10 12:00:00

"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260310_0005"
down_revision = "20260310_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "backtest_runs",
        sa.Column("idempotency_key", sa.String(80), nullable=True),
    )
    op.add_column(
        "backtest_runs",
        sa.Column("celery_task_id", sa.String(64), nullable=True),
    )
    op.add_column(
        "backtest_runs",
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_unique_constraint(
        "uq_backtest_runs_user_idempotency_key",
        "backtest_runs",
        ["user_id", "idempotency_key"],
    )
    op.create_index(
        "ix_backtest_runs_celery_task_id",
        "backtest_runs",
        ["celery_task_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_backtest_runs_celery_task_id", table_name="backtest_runs")
    op.drop_constraint("uq_backtest_runs_user_idempotency_key", "backtest_runs", type_="unique")
    op.drop_column("backtest_runs", "started_at")
    op.drop_column("backtest_runs", "celery_task_id")
    op.drop_column("backtest_runs", "idempotency_key")
