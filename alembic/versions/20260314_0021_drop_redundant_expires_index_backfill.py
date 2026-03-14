"""Drop redundant single-column expires_at index (covered by composite)
and backfill expires_at for any export jobs that lack it.

Revision ID: 20260314_0021
Revises: 20260314_0020
Create Date: 2026-03-14
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260314_0021"
down_revision = "20260314_0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_export_jobs_expires_at", table_name="export_jobs")

    op.execute(
        sa.text(
            "UPDATE export_jobs "
            "SET expires_at = created_at + INTERVAL '30 days' "
            "WHERE expires_at IS NULL"
        )
    )


def downgrade() -> None:
    op.create_index(
        "ix_export_jobs_expires_at",
        "export_jobs",
        ["expires_at"],
    )
