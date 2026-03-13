"""add composite index (status, created_at) on nightly_pipeline_runs

Revision ID: 20260313_0015
Revises: 20260313_0014
Create Date: 2026-03-13 18:00:00

"""

from __future__ import annotations

from alembic import op

revision = "20260313_0015"
down_revision = "20260313_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_nightly_pipeline_runs_status_created",
        "nightly_pipeline_runs",
        ["status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_nightly_pipeline_runs_status_created", table_name="nightly_pipeline_runs")
