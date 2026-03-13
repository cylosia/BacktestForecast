"""add indexes on export_jobs.backtest_run_id and daily_recommendations.pipeline_run_id,
add server_default to status columns

Revision ID: 20260313_0016
Revises: 20260313_0015
Create Date: 2026-03-13 20:00:00

"""

from __future__ import annotations

from alembic import op

revision = "20260313_0016"
down_revision = "20260313_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_export_jobs_backtest_run_id",
        "export_jobs",
        ["backtest_run_id"],
    )
    op.create_index(
        "ix_daily_recs_pipeline_run_id",
        "daily_recommendations",
        ["pipeline_run_id"],
    )
    op.alter_column("backtest_runs", "status", server_default="queued")
    op.alter_column("scanner_jobs", "status", server_default="queued")
    op.alter_column("export_jobs", "status", server_default="queued")
    op.alter_column("nightly_pipeline_runs", "status", server_default="running")
    op.alter_column("symbol_analyses", "status", server_default="queued")


def downgrade() -> None:
    op.alter_column("symbol_analyses", "status", server_default=None)
    op.alter_column("nightly_pipeline_runs", "status", server_default=None)
    op.alter_column("export_jobs", "status", server_default=None)
    op.alter_column("scanner_jobs", "status", server_default=None)
    op.alter_column("backtest_runs", "status", server_default=None)
    op.drop_index("ix_daily_recs_pipeline_run_id", table_name="daily_recommendations")
    op.drop_index("ix_export_jobs_backtest_run_id", table_name="export_jobs")
