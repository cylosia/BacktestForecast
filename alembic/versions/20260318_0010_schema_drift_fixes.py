"""Fix schema drift: add error_code, fix status constraint/default, add missing index and triggers.

Revision ID: 20260318_0010
Revises: 20260318_0009
Create Date: 2026-03-18
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260318_0010"
down_revision = "20260318_0009"
branch_labels = None
depends_on = None

_TRIGGER_TABLES = ("scanner_recommendations", "sweep_jobs", "sweep_results")


def upgrade() -> None:
    # 1. Add missing error_code column
    op.add_column(
        "nightly_pipeline_runs",
        sa.Column("error_code", sa.String(64), nullable=True),
    )

    # 2. Fix status CHECK constraint to include 'cancelled'
    op.drop_constraint(
        "ck_nightly_pipeline_runs_valid_pipeline_status",
        "nightly_pipeline_runs",
        type_="check",
    )
    op.create_check_constraint(
        "ck_nightly_pipeline_runs_valid_pipeline_status",
        "nightly_pipeline_runs",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
    )

    # 3. Fix server_default from 'running' to 'queued'
    op.alter_column(
        "nightly_pipeline_runs",
        "status",
        server_default="queued",
    )

    # 4. Add missing sweep_jobs index
    op.create_index(
        "ix_sweep_jobs_user_symbol",
        "sweep_jobs",
        ["user_id", "symbol"],
    )

    # 5. Add missing sweep_results index for listing queries
    op.create_index(
        "ix_sweep_results_sweep_job_id",
        "sweep_results",
        ["sweep_job_id"],
    )

    # 6. Add updated_at triggers for tables that lack them
    for table in _TRIGGER_TABLES:
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_updated_at ON {table};")
        op.execute(
            f"""
            CREATE TRIGGER trg_{table}_updated_at
            BEFORE UPDATE ON {table}
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
            """
        )


def downgrade() -> None:
    for table in reversed(_TRIGGER_TABLES):
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_updated_at ON {table};")

    op.drop_index("ix_sweep_results_sweep_job_id", table_name="sweep_results", if_exists=True)
    op.drop_index("ix_sweep_jobs_user_symbol", table_name="sweep_jobs")

    op.alter_column(
        "nightly_pipeline_runs",
        "status",
        server_default="running",
    )

    op.drop_constraint(
        "ck_nightly_pipeline_runs_valid_pipeline_status",
        "nightly_pipeline_runs",
        type_="check",
    )
    op.create_check_constraint(
        "ck_nightly_pipeline_runs_valid_pipeline_status",
        "nightly_pipeline_runs",
        "status IN ('queued', 'running', 'succeeded', 'failed')",
    )

    op.drop_column("nightly_pipeline_runs", "error_code")
