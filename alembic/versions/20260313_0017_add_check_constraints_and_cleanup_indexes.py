"""add CheckConstraints from models.py, add uq_daily_recs_pipeline_rank,
drop redundant non-unique indexes

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
    # CheckConstraints matching models.py definitions (idempotent via IF NOT EXISTS on PG)
    op.create_check_constraint(
        "valid_plan_tier", "users",
        "plan_tier IN ('free', 'pro', 'premium')",
    )
    op.create_check_constraint(
        "valid_run_status", "backtest_runs",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
    )
    op.create_check_constraint(
        "valid_job_status", "scanner_jobs",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
    )
    op.create_check_constraint(
        "valid_export_status", "export_jobs",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
    )
    op.create_check_constraint(
        "valid_pipeline_status", "nightly_pipeline_runs",
        "status IN ('running', 'succeeded', 'failed')",
    )
    op.create_check_constraint(
        "valid_analysis_status", "symbol_analyses",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
    )

    # UniqueConstraint that models.py declares but was not yet migrated
    op.create_unique_constraint(
        "uq_daily_recs_pipeline_rank", "daily_recommendations",
        ["pipeline_run_id", "rank"],
    )

    # The non-unique index is now redundant because the unique constraint
    # creates an implicit unique index on the same columns.
    op.drop_index(
        "ix_daily_recs_pipeline_run_id",
        table_name="daily_recommendations",
    )

    # Drop redundant non-unique index on backtest_equity_points — the
    # uq_backtest_equity_points_run_date unique constraint already covers
    # lookups by (run_id, trade_date).
    # Only drop if the index exists (it may have been created in an earlier migration).
    try:
        op.drop_index(
            "ix_backtest_equity_points_run_id",
            table_name="backtest_equity_points",
        )
    except Exception:
        pass


def downgrade() -> None:
    op.create_index(
        "ix_backtest_equity_points_run_id",
        "backtest_equity_points",
        ["run_id"],
    )
    op.create_index(
        "ix_daily_recs_pipeline_run_id",
        "daily_recommendations",
        ["pipeline_run_id"],
    )
    op.drop_constraint("uq_daily_recs_pipeline_rank", "daily_recommendations", type_="unique")
    op.drop_constraint("valid_analysis_status", "symbol_analyses", type_="check")
    op.drop_constraint("valid_pipeline_status", "nightly_pipeline_runs", type_="check")
    op.drop_constraint("valid_export_status", "export_jobs", type_="check")
    op.drop_constraint("valid_job_status", "scanner_jobs", type_="check")
    op.drop_constraint("valid_run_status", "backtest_runs", type_="check")
    op.drop_constraint("valid_plan_tier", "users", type_="check")
