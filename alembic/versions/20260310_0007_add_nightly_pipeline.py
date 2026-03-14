"""Add nightly pipeline and daily recommendations tables.

Revision ID: 20260310_0007
Revises: 20260310_0006
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

revision = "20260310_0007"
down_revision = "20260310_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "nightly_pipeline_runs",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="running"),
        sa.Column("stage", sa.String(32), nullable=False, server_default="universe_screen"),
        sa.Column("symbols_screened", sa.Integer, nullable=False, server_default="0"),
        sa.Column("symbols_after_screen", sa.Integer, nullable=False, server_default="0"),
        sa.Column("pairs_generated", sa.Integer, nullable=False, server_default="0"),
        sa.Column("quick_backtests_run", sa.Integer, nullable=False, server_default="0"),
        sa.Column("full_backtests_run", sa.Integer, nullable=False, server_default="0"),
        sa.Column("recommendations_produced", sa.Integer, nullable=False, server_default="0"),
        sa.Column("duration_seconds", sa.Numeric(10, 2), nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("stage_details_json", JSON_VARIANT, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_nightly_pipeline_runs_trade_date", "nightly_pipeline_runs", ["trade_date"])
    op.create_index("ix_nightly_pipeline_runs_status", "nightly_pipeline_runs", ["status"])

    op.create_table(
        "daily_recommendations",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column(
            "pipeline_run_id", GUID(), sa.ForeignKey("nightly_pipeline_runs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("rank", sa.Integer, nullable=False),
        sa.Column("score", sa.Numeric(18, 6), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("strategy_type", sa.String(64), nullable=False),
        sa.Column("regime_labels", sa.String(255), nullable=False),
        sa.Column("close_price", sa.Numeric(18, 4), nullable=False),
        sa.Column("target_dte", sa.Integer, nullable=False),
        sa.Column("config_snapshot_json", JSON_VARIANT, nullable=False, server_default="{}"),
        sa.Column("summary_json", JSON_VARIANT, nullable=False, server_default="{}"),
        sa.Column("forecast_json", JSON_VARIANT, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_daily_recs_pipeline_rank", "daily_recommendations", ["pipeline_run_id", "rank"])
    op.create_index("ix_daily_recs_trade_date", "daily_recommendations", ["trade_date"])
    op.create_index("ix_daily_recs_symbol_strategy", "daily_recommendations", ["symbol", "strategy_type"])


def downgrade() -> None:
    op.drop_table("daily_recommendations")
    op.drop_table("nightly_pipeline_runs")
