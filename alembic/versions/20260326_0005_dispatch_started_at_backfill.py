"""Add dispatch_started_at to legacy async job tables.

Revision ID: 20260326_0005
Revises: 20260326_0004
Create Date: 2026-03-26 14:45:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.exc import NoSuchTableError

from alembic import op

revision = "20260326_0005"
down_revision = "20260326_0004"
branch_labels = None
depends_on = None


_TABLES = (
    ("backtest_runs", "ix_backtest_runs_dispatch_started_at"),
    ("export_jobs", "ix_export_jobs_dispatch_started_at"),
    ("symbol_analyses", "ix_symbol_analyses_dispatch_started_at"),
    ("sweep_jobs", "ix_sweep_jobs_dispatch_started_at"),
)


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    for table_name, index_name in _TABLES:
        try:
            existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
        except NoSuchTableError:
            continue
        if "dispatch_started_at" not in existing_columns:
            op.add_column(table_name, sa.Column("dispatch_started_at", sa.DateTime(timezone=True), nullable=True))

        existing_indexes = {index["name"] for index in inspector.get_indexes(table_name)}
        if index_name not in existing_indexes:
            op.create_index(index_name, table_name, ["dispatch_started_at"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    for table_name, index_name in _TABLES:
        try:
            existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
            existing_indexes = {index["name"] for index in inspector.get_indexes(table_name)}
        except NoSuchTableError:
            continue
        if index_name in existing_indexes:
            op.drop_index(index_name, table_name=table_name)
        if "dispatch_started_at" in existing_columns:
            op.drop_column(table_name, "dispatch_started_at")
