"""add check constraints for status columns and unique constraint on daily_recs rank

Revision ID: 20260313_0017
Revises: 20260313_0016
Create Date: 2026-03-13 21:00:00

"""

from __future__ import annotations

from alembic import op

revision = "20260313_0017"
down_revision = "20260313_0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_check_constraint(
        "valid_plan_tier",
        "users",
        "plan_tier IN ('free', 'pro', 'premium')",
    )
    op.create_check_constraint(
        "valid_run_status",
        "backtest_runs",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
    )
    op.create_check_constraint(
        "valid_job_status",
        "scanner_jobs",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
    )
    op.create_check_constraint(
        "valid_export_status",
        "export_jobs",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
    )
    op.create_check_constraint(
        "valid_analysis_status",
        "symbol_analyses",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
    )
    op.create_check_constraint(
        "valid_pipeline_status",
        "nightly_pipeline_runs",
        "status IN ('running', 'succeeded', 'failed')",
    )

    op.drop_index("ix_daily_recs_pipeline_rank", table_name="daily_recommendations")
    op.create_unique_constraint(
        "uq_daily_recs_pipeline_rank",
        "daily_recommendations",
        ["pipeline_run_id", "rank"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_daily_recs_pipeline_rank", "daily_recommendations", type_="unique")
    op.create_index(
        "ix_daily_recs_pipeline_rank",
        "daily_recommendations",
        ["pipeline_run_id", "rank"],
    )

    op.drop_constraint("valid_pipeline_status", "nightly_pipeline_runs", type_="check")
    op.drop_constraint("valid_analysis_status", "symbol_analyses", type_="check")
    op.drop_constraint("valid_export_status", "export_jobs", type_="check")
    op.drop_constraint("valid_job_status", "scanner_jobs", type_="check")
    op.drop_constraint("valid_run_status", "backtest_runs", type_="check")
    op.drop_constraint("valid_plan_tier", "users", type_="check")
