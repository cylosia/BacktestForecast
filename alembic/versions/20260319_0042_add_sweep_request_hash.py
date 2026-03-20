"""Add request_hash column to sweep_jobs for SQL-level duplicate detection.

Mirrors the existing request_hash column on scanner_jobs. Enables
find_recent_duplicate to filter in SQL instead of recomputing SHA-256
in Python for each candidate row.

Revision ID: 20260319_0042
Revises: 20260319_0041
Create Date: 2026-03-19
"""
from alembic import op
import sqlalchemy as sa

revision = "20260319_0042"
down_revision = "20260319_0041"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "sweep_jobs",
        sa.Column("request_hash", sa.String(64), nullable=True),
    )
    op.create_index(
        "ix_sweep_jobs_request_hash",
        "sweep_jobs",
        ["request_hash"],
    )


def downgrade() -> None:
    op.drop_index("ix_sweep_jobs_request_hash", table_name="sweep_jobs")
    op.drop_column("sweep_jobs", "request_hash")
