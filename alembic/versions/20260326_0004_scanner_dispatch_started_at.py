"""Add dispatch_started_at to scanner jobs.

Revision ID: 20260326_0004
Revises: 20260325_0003
Create Date: 2026-03-26 11:45:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.exc import NoSuchTableError

from alembic import op

revision = "20260326_0004"
down_revision = "20260325_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        existing_columns = {column["name"] for column in inspector.get_columns("scanner_jobs")}
    except NoSuchTableError:
        return
    if "dispatch_started_at" in existing_columns:
        return

    op.add_column("scanner_jobs", sa.Column("dispatch_started_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index("ix_scanner_jobs_dispatch_started_at", "scanner_jobs", ["dispatch_started_at"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        existing_columns = {column["name"] for column in inspector.get_columns("scanner_jobs")}
    except NoSuchTableError:
        return
    if "dispatch_started_at" not in existing_columns:
        return

    existing_indexes = {index["name"] for index in inspector.get_indexes("scanner_jobs")}
    if "ix_scanner_jobs_dispatch_started_at" in existing_indexes:
        op.drop_index("ix_scanner_jobs_dispatch_started_at", table_name="scanner_jobs")
    op.drop_column("scanner_jobs", "dispatch_started_at")
