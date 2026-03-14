"""Add composite index on export_jobs(status, expires_at) for cleanup query.

Revision ID: 20260314_0020
Revises: 20260313_0019
Create Date: 2026-03-14

"""
from __future__ import annotations

from alembic import op


revision = "20260314_0020"
down_revision = "20260313_0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_export_jobs_status_expires_at",
        "export_jobs",
        ["status", "expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_export_jobs_status_expires_at", table_name="export_jobs")
