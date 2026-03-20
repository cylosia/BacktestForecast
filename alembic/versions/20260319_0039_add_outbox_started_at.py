"""Add started_at column to outbox_messages.

Tracks when an outbox message dispatch attempt began, enabling
latency monitoring and stale-message detection.

Revision ID: 20260319_0039
Revises: 20260319_0038
"""
from alembic import op
import sqlalchemy as sa


revision = "20260319_0039"
down_revision = "20260319_0038"
branch_labels = None
depends_on = None


def _column_exists(table: str, column: str) -> bool:
    bind = op.get_bind()
    row = bind.execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :tbl AND column_name = :col"
        ),
        {"tbl": table, "col": column},
    ).fetchone()
    return row is not None


def upgrade() -> None:
    if not _column_exists("outbox_messages", "started_at"):
        op.add_column(
            "outbox_messages",
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        )


def downgrade() -> None:
    if _column_exists("outbox_messages", "started_at"):
        op.drop_column("outbox_messages", "started_at")
