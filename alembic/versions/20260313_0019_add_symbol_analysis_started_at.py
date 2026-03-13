"""Add started_at to symbol_analyses.

The stale-job reaper needs to distinguish between jobs that have been
queued for a long time and jobs that have been *running* for a long time.
Without a started_at column on SymbolAnalysis it had to use created_at
as a proxy, which could prematurely reap analyses that queued for a while.

Revision ID: 20260313_0019
Revises: 20260313_0018
Create Date: 2026-03-13

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260313_0019"
down_revision = "20260313_0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "symbol_analyses",
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("symbol_analyses", "started_at")
