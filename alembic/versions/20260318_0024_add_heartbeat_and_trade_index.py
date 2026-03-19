"""Add last_heartbeat_at columns and backtest_trades run_id index.

Revision ID: 0024_heartbeat
Revises: 20260318_0025
Create Date: 2026-03-18
"""
from alembic import op
import sqlalchemy as sa

revision = "0024_heartbeat"
down_revision = "20260318_0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in ("backtest_runs", "scanner_jobs", "export_jobs", "symbol_analyses", "sweep_jobs"):
        op.add_column(table, sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True))

    op.create_index("ix_backtest_trades_run_id", "backtest_trades", ["run_id"])


def downgrade() -> None:
    op.drop_index("ix_backtest_trades_run_id", table_name="backtest_trades")

    for table in ("sweep_jobs", "symbol_analyses", "export_jobs", "scanner_jobs", "backtest_runs"):
        op.drop_column(table, "last_heartbeat_at")
