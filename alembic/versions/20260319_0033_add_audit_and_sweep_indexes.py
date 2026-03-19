"""Add missing audit_events and sweep_jobs indexes.

Revision ID: 20260319_0030
Revises: 0024_heartbeat
Create Date: 2026-03-19
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
        op.create_index(
            "ix_audit_events_created_at",
            "audit_events",
            ["created_at"],
            postgresql_concurrently=True,
        )

    if not _index_exists("ix_sweep_jobs_user_symbol_created"):
        op.create_index(
            "ix_sweep_jobs_user_symbol_created",
            "sweep_jobs",
            ["user_id", "symbol", "created_at"],
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    if _index_exists("ix_sweep_jobs_user_symbol_created"):
        op.drop_index("ix_sweep_jobs_user_symbol_created", table_name="sweep_jobs")
    if _index_exists("ix_audit_events_created_at"):
        op.drop_index("ix_audit_events_created_at", table_name="audit_events")
