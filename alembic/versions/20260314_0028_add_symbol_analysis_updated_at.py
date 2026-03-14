"""Add updated_at column to symbol_analyses.

Revision ID: 20260314_0028
Revises: 20260314_0027
Create Date: 2026-03-14
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260314_0028"
down_revision = "20260314_0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "symbol_analyses",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("symbol_analyses", "updated_at")
