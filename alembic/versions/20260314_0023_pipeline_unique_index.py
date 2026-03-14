"""Add unique partial index on pipeline_runs(trade_date) for succeeded runs.

Revision ID: 0023_pipeline_unique
Revises: 20260314_0022
Create Date: 2026-03-14
"""
from __future__ import annotations

from alembic import op

revision = "20260314_0023"
down_revision = "20260314_0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_nightly_pipeline_runs_trade_date_succeeded",
        "nightly_pipeline_runs",
        ["trade_date"],
        unique=True,
        postgresql_where="status = 'succeeded'",
    )


def downgrade() -> None:
    op.drop_index("ix_nightly_pipeline_runs_trade_date_succeeded", table_name="nightly_pipeline_runs")
