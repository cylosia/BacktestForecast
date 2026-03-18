"""Add mode column to sweep_jobs and fix boolean server_default consistency.

Revision ID: 20260318_0022
Revises: 20260318_0021
Create Date: 2026-03-18

Adds a queryable ``mode`` column ('grid' or 'genetic') to sweep_jobs so sweeps
can be filtered by mode without parsing the JSON request snapshot.  Also fixes
the scanner_jobs.refresh_daily server_default for Alembic autogenerate parity.
"""
import sqlalchemy as sa
from alembic import op

revision = "20260318_0022"
down_revision = "20260318_0021"
branch_labels = None
depends_on = None


def _add_column_if_not_exists(table: str, column_name: str, column: sa.Column) -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    existing = [c["name"] for c in insp.get_columns(table)]
    if column_name not in existing:
        op.add_column(table, column)


def _check_exists(table: str, name: str) -> bool:
    bind = op.get_bind()
    result = bind.execute(
        sa.text(
            "SELECT 1 FROM information_schema.table_constraints "
            "WHERE table_name = :tbl AND constraint_name = :name"
        ),
        {"tbl": table, "name": name},
    )
    return result.fetchone() is not None


def upgrade() -> None:
    _add_column_if_not_exists(
        "sweep_jobs",
        "mode",
        sa.Column("mode", sa.String(16), nullable=False, server_default="grid"),
    )

    if not _check_exists("sweep_jobs", "ck_sweep_jobs_valid_mode"):
        op.execute(sa.text(
            "ALTER TABLE sweep_jobs "
            "ADD CONSTRAINT ck_sweep_jobs_valid_mode "
            "CHECK (mode IN ('grid', 'genetic')) NOT VALID"
        ))

    op.execute(
        sa.text(
            "UPDATE sweep_jobs SET mode = "
            "COALESCE(request_snapshot_json->>'mode', 'grid') "
            "WHERE mode = 'grid' "
            "AND request_snapshot_json->>'mode' IS NOT NULL "
            "AND request_snapshot_json->>'mode' != 'grid'"
        )
    )

    op.alter_column(
        "scanner_jobs",
        "refresh_daily",
        server_default=sa.text("false"),
    )


def downgrade() -> None:
    op.execute("ALTER TABLE sweep_jobs DROP CONSTRAINT IF EXISTS ck_sweep_jobs_valid_mode")
    op.drop_column("sweep_jobs", "mode")

    op.alter_column(
        "scanner_jobs",
        "refresh_daily",
        server_default="false",
    )
