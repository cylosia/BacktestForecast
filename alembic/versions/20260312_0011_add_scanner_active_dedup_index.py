"""add scanner active dedup partial unique index

Revision ID: 20260312_0011
Revises: 20260312_0010
Create Date: 2026-03-12 18:00:00

"""

from __future__ import annotations

from alembic import op

revision = "20260312_0011"
down_revision = "20260312_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "uq_scanner_jobs_active_dedup",
        "scanner_jobs",
        ["user_id", "request_hash", "mode"],
        unique=True,
        postgresql_where="status IN ('queued', 'running')",
    )


def downgrade() -> None:
    op.drop_index("uq_scanner_jobs_active_dedup", table_name="scanner_jobs")
