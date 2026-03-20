"""Add missing audit_events and sweep_jobs indexes.

Revision ID: 20260319_0033
Revises: 20260319_0032
Create Date: 2026-03-19

NOTE: Uses non-CONCURRENTLY indexes within the transaction block.
For zero-downtime on very large tables, consider running index creation
manually outside a transaction using CREATE INDEX CONCURRENTLY.
"""
from alembic import op
import sqlalchemy as sa

revision = "20260319_0033"
down_revision = "20260319_0032"
branch_labels = None
depends_on = None


def _index_exists(name: str) -> bool:
    bind = op.get_bind()
    row = bind.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE indexname = :name"),
        {"name": name},
    ).fetchone()
    return row is not None


def upgrade() -> None:
    if not _index_exists("ix_audit_events_created_at"):
        op.execute(sa.text(
            "CREATE INDEX IF NOT EXISTS ix_audit_events_created_at "
            "ON audit_events (created_at)"
        ))

    if not _index_exists("ix_sweep_jobs_user_symbol_created"):
        op.execute(sa.text(
            "CREATE INDEX IF NOT EXISTS ix_sweep_jobs_user_symbol_created "
            "ON sweep_jobs (user_id, symbol, created_at)"
        ))


def downgrade() -> None:
    if _index_exists("ix_sweep_jobs_user_symbol_created"):
        op.drop_index("ix_sweep_jobs_user_symbol_created", table_name="sweep_jobs")
    if _index_exists("ix_audit_events_created_at"):
        op.drop_index("ix_audit_events_created_at", table_name="audit_events")
