"""add historical raw underlying day bars

Revision ID: 20260409_0016
Revises: 20260331_0015
Create Date: 2026-04-09 10:30:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

from backtestforecast.db.types import GUID

revision = "20260409_0016"
down_revision = "20260331_0015"
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

    if "historical_underlying_raw_day_bars" in tables:
        return

    op.create_table(
        "historical_underlying_raw_day_bars",
        sa.Column("id", GUID(), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("open_price", sa.Numeric(18, 6), nullable=False),
        sa.Column("high_price", sa.Numeric(18, 6), nullable=False),
        sa.Column("low_price", sa.Numeric(18, 6), nullable=False),
        sa.Column("close_price", sa.Numeric(18, 6), nullable=False),
        sa.Column("volume", sa.Numeric(24, 4), nullable=False),
        sa.Column("source_dataset", sa.String(length=64), nullable=False, server_default="massive_rest_day_aggs_raw"),
        sa.Column("source_file_date", sa.Date(), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("length(symbol) > 0", name="ck_historical_underlying_raw_day_bars_symbol_not_empty"),
        sa.CheckConstraint("open_price > 0", name="ck_historical_underlying_raw_day_bars_open_positive"),
        sa.CheckConstraint("high_price > 0", name="ck_historical_underlying_raw_day_bars_high_positive"),
        sa.CheckConstraint("low_price > 0", name="ck_historical_underlying_raw_day_bars_low_positive"),
        sa.CheckConstraint("close_price > 0", name="ck_historical_underlying_raw_day_bars_close_positive"),
        sa.CheckConstraint("volume >= 0", name="ck_historical_underlying_raw_day_bars_volume_nonneg"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "trade_date", name="uq_historical_underlying_raw_day_bars_symbol_date"),
    )
    op.create_index(
        "ix_historical_underlying_raw_day_bars_covering",
        "historical_underlying_raw_day_bars",
        ["symbol", "trade_date"],
        unique=False,
        postgresql_include=["open_price", "high_price", "low_price", "close_price", "volume"],
    )
    op.create_index(
        "ix_historical_underlying_raw_day_bars_trade_date_desc",
        "historical_underlying_raw_day_bars",
        [sa.text("trade_date DESC")],
        unique=False,
    )
    _create_updated_at_trigger("historical_underlying_raw_day_bars")


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = set(inspector.get_table_names())
    if "historical_underlying_raw_day_bars" not in tables:
        return
    if bind.dialect.name == "postgresql":
        op.execute(
            "DROP TRIGGER IF EXISTS trg_historical_underlying_raw_day_bars_updated_at ON historical_underlying_raw_day_bars;"
        )
    op.drop_index("ix_historical_underlying_raw_day_bars_trade_date_desc", table_name="historical_underlying_raw_day_bars")
    op.drop_index("ix_historical_underlying_raw_day_bars_covering", table_name="historical_underlying_raw_day_bars")
    op.drop_table("historical_underlying_raw_day_bars")
