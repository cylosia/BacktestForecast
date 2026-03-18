"""Drop duplicate ix_sweep_results_sweep_job_id index.

ix_sweep_results_job_id (from migration 0006) already covers the same column.

Revision ID: 20260318_0015
Revises: 20260318_0014
Create Date: 2026-03-18
"""
from __future__ import annotations

from alembic import op

revision = "20260318_0015"
down_revision = "20260318_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_sweep_results_sweep_job_id", table_name="sweep_results", if_exists=True)


def downgrade() -> None:
    op.create_index(
        "ix_sweep_results_sweep_job_id",
        "sweep_results",
        ["sweep_job_id"],
    )
