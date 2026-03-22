"""add dispatch_started_at to job tables

Revision ID: 20260322_0047
Revises: 20260320_0046
Create Date: 2026-03-22 00:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260322_0047"
down_revision = "20260320_0046"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table_name in (
        "backtest_runs",
        "scanner_jobs",
        "export_jobs",
        "symbol_analyses",
        "sweep_jobs",
    ):
        op.add_column(table_name, sa.Column("dispatch_started_at", sa.DateTime(timezone=True), nullable=True))

    op.create_index("ix_backtest_runs_dispatch_started_at", "backtest_runs", ["dispatch_started_at"], unique=False)
    op.create_index("ix_scanner_jobs_dispatch_started_at", "scanner_jobs", ["dispatch_started_at"], unique=False)
    op.create_index("ix_export_jobs_dispatch_started_at", "export_jobs", ["dispatch_started_at"], unique=False)
    op.create_index("ix_symbol_analyses_dispatch_started_at", "symbol_analyses", ["dispatch_started_at"], unique=False)
    op.create_index("ix_sweep_jobs_dispatch_started_at", "sweep_jobs", ["dispatch_started_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_sweep_jobs_dispatch_started_at", table_name="sweep_jobs")
    op.drop_index("ix_symbol_analyses_dispatch_started_at", table_name="symbol_analyses")
    op.drop_index("ix_export_jobs_dispatch_started_at", table_name="export_jobs")
    op.drop_index("ix_scanner_jobs_dispatch_started_at", table_name="scanner_jobs")
    op.drop_index("ix_backtest_runs_dispatch_started_at", table_name="backtest_runs")

    for table_name in (
        "sweep_jobs",
        "symbol_analyses",
        "export_jobs",
        "scanner_jobs",
        "backtest_runs",
    ):
        op.drop_column(table_name, "dispatch_started_at")
