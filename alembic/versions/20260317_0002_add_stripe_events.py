"""Add stripe_events table for dedicated webhook event persistence.

Revision ID: 20260317_0002
Revises: 20260315_0001
Create Date: 2026-03-17
"""
from __future__ import annotations

import uuid

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.types import CHAR, TypeDecorator


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

revision = "20260317_0002"
down_revision = "20260315_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "stripe_events",
        sa.Column("id", GUID(), nullable=False, default=uuid.uuid4),
        sa.Column("stripe_event_id", sa.String(255), nullable=False),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column("livemode", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("idempotency_status", sa.String(16), nullable=False, server_default="processed"),
        sa.Column("user_id", GUID(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("request_id", sa.String(64), nullable=True),
        sa.Column("ip_hash", sa.String(128), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("payload_summary", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("stripe_event_id", name="uq_stripe_events_event_id"),
        sa.CheckConstraint(
            "idempotency_status IN ('processed', 'ignored', 'error')",
            name="ck_stripe_events_valid_status",
        ),
    )
    op.create_index("ix_stripe_events_event_type", "stripe_events", ["event_type"])
    op.create_index("ix_stripe_events_created_at", "stripe_events", ["created_at"])
    op.create_index("ix_stripe_events_user_id", "stripe_events", ["user_id"])


def downgrade() -> None:
    op.drop_index("ix_stripe_events_user_id", table_name="stripe_events")
    op.drop_index("ix_stripe_events_created_at", table_name="stripe_events")
    op.drop_index("ix_stripe_events_event_type", table_name="stripe_events")
    op.drop_table("stripe_events")
