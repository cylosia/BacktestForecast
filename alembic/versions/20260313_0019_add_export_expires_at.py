"""Add expires_at to export_jobs, started_at to symbol_analyses, storage_key
to export_jobs, and expand valid_export_status.

Revision ID: 20260313_0019
Revises: 20260313_0018
Create Date: 2026-03-13

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260313_0019"
down_revision = "20260313_0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "export_jobs",
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "export_jobs",
        sa.Column("storage_key", sa.String(512), nullable=True),
    )
    op.add_column(
        "symbol_analyses",
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_export_jobs_expires_at",
        "export_jobs",
        ["expires_at"],
    )
    op.drop_constraint("ck_export_jobs_valid_export_status", "export_jobs", type_="check")
    op.create_check_constraint(
        "ck_export_jobs_valid_export_status",
        "export_jobs",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_export_jobs_valid_export_status", "export_jobs", type_="check")
    op.create_check_constraint(
        "ck_export_jobs_valid_export_status",
        "export_jobs",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
    )
    op.drop_index("ix_export_jobs_expires_at", table_name="export_jobs")
    op.drop_column("symbol_analyses", "started_at")
    op.drop_column("export_jobs", "storage_key")
    op.drop_column("export_jobs", "expires_at")
