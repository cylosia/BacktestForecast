"""add started_at to export_jobs

Revision ID: 20260313_0013
Revises: 20260313_0012
Create Date: 2026-03-13 14:00:00

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260313_0013"
down_revision = "20260313_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("export_jobs", sa.Column("started_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("export_jobs", "started_at")
