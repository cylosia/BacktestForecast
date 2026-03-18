"""Fix stripe_events.updated_at: nullable=False and onupdate pattern.

Revision ID: 20260318_0016
Revises: 20260318_0015
Create Date: 2026-03-18

- Backfill NULL updated_at with now()
- Alter column to nullable=False
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260318_0016"
down_revision = "20260318_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE stripe_events SET updated_at = now() WHERE updated_at IS NULL")
    op.alter_column(
        "stripe_events",
        "updated_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "stripe_events",
        "updated_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=True,
    )
