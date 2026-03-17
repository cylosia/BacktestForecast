"""Add export_format CHECK constraint.

Revision ID: 20260317_0004
Revises: 20260317_0003
Create Date: 2026-03-17
"""
from __future__ import annotations

from alembic import op

revision = "20260317_0004"
down_revision = "20260317_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_export_jobs_valid_export_format",
        "export_jobs",
        "export_format IN ('csv', 'pdf')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_export_jobs_valid_export_format", "export_jobs", type_="check")
