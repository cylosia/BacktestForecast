"""Add last_heartbeat_at columns and backtest_trades run_id index.

Revision ID: 0024_heartbeat
Revises: 20260318_0027
Create Date: 2026-03-18
"""
from alembic import op
import sqlalchemy as sa

revision = "0024_heartbeat"
down_revision = "20260318_0027"
branch_labels = None
depends_on = None

_HEARTBEAT_TABLES = (
    "backtest_runs", "scanner_jobs", "export_jobs",
    "symbol_analyses", "sweep_jobs", "nightly_pipeline_runs",
)


def _column_exists(table: str, column: str) -> bool:
    bind = op.get_bind()
    row = bind.execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :tbl AND column_name = :col"
        ),
        {"tbl": table, "col": column},
    ).fetchone()
    return row is not None


def _index_exists(name: str) -> bool:
    bind = op.get_bind()
    row = bind.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE indexname = :name"),
        {"name": name},
    ).fetchone()
    return row is not None


def upgrade() -> None:
    for table in _HEARTBEAT_TABLES:
        if not _column_exists(table, "last_heartbeat_at"):
            op.add_column(table, sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True))

    if not _index_exists("ix_backtest_trades_run_id"):
        op.create_index("ix_backtest_trades_run_id", "backtest_trades", ["run_id"])


def downgrade() -> None:
    if _index_exists("ix_backtest_trades_run_id"):
        op.drop_index("ix_backtest_trades_run_id", table_name="backtest_trades")

    for table in reversed(_HEARTBEAT_TABLES):
        if _column_exists(table, "last_heartbeat_at"):
            op.drop_column(table, "last_heartbeat_at")
