"""add composite indexes for query performance

Revision ID: 20260312_0010
Revises: 20260311_0009
Create Date: 2026-03-12 10:00:00

"""

from __future__ import annotations

from alembic import op

revision = "20260312_0010"
down_revision = "20260311_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_backtest_templates_user_updated_at",
        "backtest_templates",
        ["user_id", "updated_at"],
    )
    op.create_index(
        "ix_scanner_jobs_dedup_lookup",
        "scanner_jobs",
        ["user_id", "request_hash", "mode", "created_at"],
    )
    op.create_index(
        "ix_scanner_jobs_refresh_sources",
        "scanner_jobs",
        ["refresh_daily", "status"],
    )
    op.create_index(
        "ix_nightly_pipeline_runs_date_status",
        "nightly_pipeline_runs",
        ["trade_date", "status"],
    )


def downgrade() -> None:
    op.drop_index("ix_nightly_pipeline_runs_date_status", table_name="nightly_pipeline_runs")
    op.drop_index("ix_scanner_jobs_refresh_sources", table_name="scanner_jobs")
    op.drop_index("ix_scanner_jobs_dedup_lookup", table_name="scanner_jobs")
    op.drop_index("ix_backtest_templates_user_updated_at", table_name="backtest_templates")
