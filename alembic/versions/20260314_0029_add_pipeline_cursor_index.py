"""Add composite index for pipeline runs cursor pagination.

Revision ID: 20260314_0029
Revises: 20260314_0028
Create Date: 2026-03-14
"""
from __future__ import annotations

from alembic import op

revision = "20260314_0029"
down_revision = "20260314_0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_nightly_pipeline_runs_cursor",
        "nightly_pipeline_runs",
        ["created_at", "id"],
    )


def downgrade() -> None:
    op.drop_index("ix_nightly_pipeline_runs_cursor", table_name="nightly_pipeline_runs")
