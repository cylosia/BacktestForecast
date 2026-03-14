"""Audit 7 cleanup: drop redundant pipeline index, add plan_tier check.

Revision ID: 20260314_0025
Revises: 20260314_0024
Create Date: 2026-03-14
"""
from __future__ import annotations

from alembic import op

revision = "20260314_0025"
down_revision = "20260314_0024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        "ix_nightly_pipeline_runs_trade_date_succeeded",
        table_name="nightly_pipeline_runs",
    )

    op.create_check_constraint(
        "ck_scanner_jobs_valid_plan_tier",
        "scanner_jobs",
        "plan_tier_snapshot IN ('free', 'pro', 'premium')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_scanner_jobs_valid_plan_tier", "scanner_jobs", type_="check")

    op.create_index(
        "ix_nightly_pipeline_runs_trade_date_succeeded",
        "nightly_pipeline_runs",
        ["trade_date"],
        unique=True,
        postgresql_where="status = 'succeeded'",
    )
