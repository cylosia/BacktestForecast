"""Add outbox_messages table for transactional outbox pattern.

Note: filename predates revision renumbering; revision ID is authoritative.

Revision ID: 20260318_0012
Revises: 20260318_0011
Create Date: 2026-03-18
"""
# NOTE: The OutboxMessage ORM model is defined in models.py. The outbox
# poller (process_outbox task) is not yet implemented — see dispatch.py.
from __future__ import annotations

import uuid

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.types import CHAR, TypeDecorator

from alembic import op


class GUID(TypeDecorator[uuid.UUID]):
    """Frozen copy of backtestforecast.db.types.GUID for migration stability."""

    impl = CHAR(36)
    cache_ok = True

    def load_dialect_impl(self, dialect):  # type: ignore[override]
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):  # type: ignore[override]
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value if dialect.name == "postgresql" else str(value)
        coerced = uuid.UUID(str(value))
        return coerced if dialect.name == "postgresql" else str(coerced)

    def process_result_value(self, value, dialect):  # type: ignore[override]
        if value is None or isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))


JSON_VARIANT = sa.JSON().with_variant(JSONB, "postgresql")

revision = "20260318_0012"
down_revision = "20260318_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbox_messages",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("task_name", sa.String(128), nullable=False),
        sa.Column("task_kwargs_json", JSON_VARIANT, nullable=False),
        sa.Column("queue", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("retry_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("correlation_id", GUID(), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'sent', 'failed')",
            name="ck_outbox_messages_valid_status",
        ),
    )
    op.create_index(
        "ix_outbox_messages_status_created",
        "outbox_messages",
        ["status", "created_at"],
    )
    op.execute(
        "CREATE TRIGGER trg_outbox_messages_updated_at "
        "BEFORE UPDATE ON outbox_messages "
        "FOR EACH ROW EXECUTE FUNCTION set_updated_at()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_outbox_messages_updated_at ON outbox_messages")
    op.drop_table("outbox_messages")
