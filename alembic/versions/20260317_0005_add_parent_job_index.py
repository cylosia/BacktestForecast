"""Add ix_scanner_jobs_parent_job_id index.

Revision ID: 20260317_0005
Revises: 20260317_0004
Create Date: 2026-03-17
"""
from __future__ import annotations

from alembic import op

revision = "20260317_0005"
down_revision = "20260317_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_scanner_jobs_parent_job_id", "scanner_jobs", ["parent_job_id"])


def downgrade() -> None:
    op.drop_index("ix_scanner_jobs_parent_job_id", table_name="scanner_jobs")
