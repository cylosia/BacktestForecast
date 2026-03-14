"""Reconcile model-migration index drift, drop duplicate index, fix updated_at nullability.

- Drop the redundant ix_daily_recs_pipeline_rank index (covered by uq_daily_recs_pipeline_rank).
- Add migration-only indexes to model __table_args__ (no DDL needed, they already exist).
- Make symbol_analyses.updated_at NOT NULL for consistency with all other models.

Revision ID: 20260314_0030
Revises: 20260314_0029
Create Date: 2026-03-14
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260314_0030"
down_revision = "20260314_0029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop redundant regular index — the unique constraint uq_daily_recs_pipeline_rank
    # already provides an implicit index on (pipeline_run_id, rank).
    op.drop_index("ix_daily_recs_pipeline_rank", table_name="daily_recommendations")

    # Backfill any NULL updated_at rows before adding NOT NULL constraint
    op.execute(
        sa.text(
            "UPDATE symbol_analyses SET updated_at = COALESCE(created_at, now()) "
            "WHERE updated_at IS NULL"
        )
    )
    op.alter_column(
        "symbol_analyses",
        "updated_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )


def downgrade() -> None:
    op.alter_column(
        "symbol_analyses",
        "updated_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.create_index(
        "ix_daily_recs_pipeline_rank",
        "daily_recommendations",
        ["pipeline_run_id", "rank"],
    )
