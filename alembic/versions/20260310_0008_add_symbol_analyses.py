"""Add symbol_analyses table for deep single-symbol analysis.

Revision ID: 20260310_0008
Revises: 20260310_0007
"""

import uuid

import sqlalchemy as sa
from sqlalchemy import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.types import CHAR, TypeDecorator

from alembic import op


class GUID(TypeDecorator[uuid.UUID]):
    impl = CHAR(36)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value if dialect.name == "postgresql" else str(value)
        coerced = uuid.UUID(str(value))
        return coerced if dialect.name == "postgresql" else str(coerced)

    def process_result_value(self, value, dialect):
        if value is None or isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))


JSON_VARIANT = JSON().with_variant(JSONB, "postgresql")

revision = "20260310_0008"
down_revision = "20260310_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "symbol_analyses",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("user_id", GUID(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="queued"),
        sa.Column("stage", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("close_price", sa.Numeric(18, 4), nullable=True),
        sa.Column("regime_json", JSON_VARIANT, nullable=False, server_default="{}"),
        sa.Column("landscape_json", JSON_VARIANT, nullable=False, server_default="[]"),
        sa.Column("top_results_json", JSON_VARIANT, nullable=False, server_default="[]"),
        sa.Column("forecast_json", JSON_VARIANT, nullable=False, server_default="{}"),
        sa.Column("strategies_tested", sa.Integer, nullable=False, server_default="0"),
        sa.Column("configs_tested", sa.Integer, nullable=False, server_default="0"),
        sa.Column("top_results_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("duration_seconds", sa.Numeric(10, 2), nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("idempotency_key", sa.String(80), nullable=True),
        sa.Column("celery_task_id", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_symbol_analyses_user_idempotency"),
    )
    op.create_index("ix_symbol_analyses_user_created", "symbol_analyses", ["user_id", "created_at"])
    op.create_index("ix_symbol_analyses_symbol", "symbol_analyses", ["symbol"])


def downgrade() -> None:
    op.drop_table("symbol_analyses")
