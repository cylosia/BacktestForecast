"""Add completed_at column to outbox_messages for delivery tracking.

Allows monitoring delivery latency (completed_at - created_at) and
identifying slow outbox recovery paths.

Revision ID: 20260319_0041
Revises: 20260319_0040
Create Date: 2026-03-19
"""
from alembic import op
import sqlalchemy as sa

revision = "20260319_0041"
down_revision = "20260319_0040"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "outbox_messages",
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("outbox_messages", "completed_at")
