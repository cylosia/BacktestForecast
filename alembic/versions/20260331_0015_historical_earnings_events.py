"""add historical earnings event storage

Revision ID: 20260331_0015
Revises: 20260331_0014
Create Date: 2026-03-31 09:05:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

from backtestforecast.db.types import GUID

revision = "20260331_0015"
down_revision = "20260331_0014"
branch_labels = None
depends_on = None


def _create_updated_at_trigger(table_name: str) -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    op.execute(
        """
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = clock_timestamp();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(f"DROP TRIGGER IF EXISTS trg_{table_name}_updated_at ON {table_name};")
    op.execute(
        f"""
        CREATE TRIGGER trg_{table_name}_updated_at
        BEFORE UPDATE ON {table_name}
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = set(inspector.get_table_names())

    if "historical_earnings_events" not in tables:
        op.create_table(
            "historical_earnings_events",
            sa.Column("id", GUID(), nullable=False),
            sa.Column("symbol", sa.String(length=32), nullable=False),
            sa.Column("event_date", sa.Date(), nullable=False),
            sa.Column("event_type", sa.String(length=48), nullable=False),
            sa.Column("provider_event_id", sa.String(length=128), nullable=True),
            sa.Column("source_dataset", sa.String(length=64), nullable=False, server_default="rest_earnings"),
            sa.Column("source_file_date", sa.Date(), nullable=False),
            sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.CheckConstraint("length(symbol) > 0", name="ck_historical_earnings_events_symbol_not_empty"),
            sa.CheckConstraint(
                "event_type IN ('earnings_announcement_date', 'earnings_conference_call')",
                name="ck_historical_earnings_events_event_type",
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("provider_event_id", name="uq_historical_earnings_events_provider_event_id"),
            sa.UniqueConstraint("symbol", "event_date", "event_type", name="uq_historical_earnings_events_symbol_date_type"),
        )
        op.create_index(
            "ix_historical_earnings_events_date_desc",
            "historical_earnings_events",
            [sa.text("event_date DESC")],
            unique=False,
        )
        _create_updated_at_trigger("historical_earnings_events")


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = set(inspector.get_table_names())
    if "historical_earnings_events" not in tables:
        return
    if bind.dialect.name == "postgresql":
        op.execute("DROP TRIGGER IF EXISTS trg_historical_earnings_events_updated_at ON historical_earnings_events;")
    op.drop_index("ix_historical_earnings_events_date_desc", table_name="historical_earnings_events")
    op.drop_table("historical_earnings_events")
