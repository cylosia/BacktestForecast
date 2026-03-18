"""Audit round 2 schema fixes.

- Add BacktestTrade CHECK constraints (dte_at_open, holding_period_days)
- Add OutboxMessage retry_count CHECK, nullable fix
- Add SweepJob.plan_tier_snapshot column and CHECK
- Add User email index
- Change ExportJob.size_bytes to BigInteger
- Add DailyRecommendation.updated_at column

Revision ID: 20260318_0017
Revises: 20260318_0016
Create Date: 2026-03-18
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260318_0017"
down_revision = "20260318_0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. BacktestTrade CHECK constraints (idempotent — skip if already present)
    _create_check_if_not_exists(
        "backtest_trades",
        "ck_backtest_trades_dte_at_open_nonneg",
        "dte_at_open >= 0",
    )
    _create_check_if_not_exists(
        "backtest_trades",
        "ck_backtest_trades_holding_period_nonneg",
        "holding_period_days >= 0",
    )

    # 2. OutboxMessage retry_count CHECK and nullable fix
    _create_check_if_not_exists(
        "outbox_messages",
        "ck_outbox_messages_retry_count_nonneg",
        "retry_count >= 0",
    )
    op.execute("UPDATE outbox_messages SET retry_count = 0 WHERE retry_count IS NULL")
    op.alter_column(
        "outbox_messages",
        "retry_count",
        existing_type=sa.Integer(),
        nullable=False,
        server_default="0",
    )

    # 3. SweepJob.plan_tier_snapshot — add column if missing, then CHECK
    _add_column_if_not_exists(
        "sweep_jobs",
        "plan_tier_snapshot",
        sa.Column("plan_tier_snapshot", sa.String(16), nullable=False, server_default="free"),
    )
    _create_check_if_not_exists(
        "sweep_jobs",
        "ck_sweep_jobs_valid_plan_tier",
        "plan_tier_snapshot IN ('free', 'pro', 'premium')",
    )

    # 4. User email index
    _create_index_if_not_exists("ix_users_email", "users", ["email"])

    # 5. ExportJob.size_bytes → BigInteger
    op.alter_column(
        "export_jobs",
        "size_bytes",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=False,
        existing_server_default="0",
    )

    # 6. DailyRecommendation.updated_at
    _add_column_if_not_exists(
        "daily_recommendations",
        "updated_at",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )


def downgrade() -> None:
    # 6. Drop DailyRecommendation.updated_at
    op.drop_column("daily_recommendations", "updated_at")

    # 5. Revert ExportJob.size_bytes to Integer
    op.alter_column(
        "export_jobs",
        "size_bytes",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=False,
        existing_server_default="0",
    )

    # 4. Drop User email index
    op.drop_index("ix_users_email", table_name="users")

    # 3. Drop SweepJob.plan_tier_snapshot CHECK and column
    op.execute("ALTER TABLE sweep_jobs DROP CONSTRAINT IF EXISTS ck_sweep_jobs_valid_plan_tier")
    op.drop_column("sweep_jobs", "plan_tier_snapshot")

    # 2. Revert OutboxMessage retry_count
    op.execute("ALTER TABLE outbox_messages DROP CONSTRAINT IF EXISTS ck_outbox_messages_retry_count_nonneg")
    op.alter_column(
        "outbox_messages",
        "retry_count",
        existing_type=sa.Integer(),
        nullable=True,
        server_default=None,
    )

    # 1. Drop BacktestTrade CHECK constraints
    op.execute("ALTER TABLE backtest_trades DROP CONSTRAINT IF EXISTS ck_backtest_trades_dte_at_open_nonneg")
    op.execute("ALTER TABLE backtest_trades DROP CONSTRAINT IF EXISTS ck_backtest_trades_holding_period_nonneg")


# ---------------------------------------------------------------------------
# Helpers for idempotent DDL
# ---------------------------------------------------------------------------

def _create_check_if_not_exists(table: str, name: str, condition: str) -> None:
    op.execute(sa.text(f"""
        DO $$ BEGIN
            ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({condition}) NOT VALID;
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$;
    """))


def _add_column_if_not_exists(table: str, column_name: str, column: sa.Column) -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    columns = [c["name"] for c in insp.get_columns(table)]
    if column_name not in columns:
        op.add_column(table, column)


def _create_index_if_not_exists(index_name: str, table: str, columns: list[str]) -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    existing = [idx["name"] for idx in insp.get_indexes(table)]
    if index_name not in existing:
        op.create_index(index_name, table, columns)
