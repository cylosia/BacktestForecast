"""Add explicit FK relationship between NightlyPipelineRun and ScannerJob.

Adds pipeline_run_id column to scanner_jobs for explicit linkage instead of
implicit trade_date matching. Also adds index for efficient lookups.

Revision ID: 0035
Revises: 0034
"""
from alembic import op
import sqlalchemy as sa
from backtestforecast.db.types import GUID


revision = "20260319_0035"
down_revision = "20260319_0034"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "scanner_jobs",
        sa.Column("pipeline_run_id", GUID(), nullable=True),
    )
    op.create_foreign_key(
        "fk_scanner_jobs_pipeline_run_id",
        "scanner_jobs",
        "nightly_pipeline_runs",
        ["pipeline_run_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_scanner_jobs_pipeline_run_id",
        "scanner_jobs",
        ["pipeline_run_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_scanner_jobs_pipeline_run_id", table_name="scanner_jobs")
    op.drop_constraint("fk_scanner_jobs_pipeline_run_id", "scanner_jobs", type_="foreignkey")
    op.drop_column("scanner_jobs", "pipeline_run_id")
