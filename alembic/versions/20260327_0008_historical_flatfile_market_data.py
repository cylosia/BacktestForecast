"""Add historical flat-file-backed market data tables.

Revision ID: 20260327_0008
Revises: 20260327_0007
Create Date: 2026-03-27 18:05:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

from backtestforecast.db.types import GUID

revision = "20260327_0008"
down_revision = "20260327_0007"
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

    if "historical_underlying_day_bars" not in tables:
        op.create_table(
            "historical_underlying_day_bars",
            sa.Column("id", GUID(), nullable=False),
            sa.Column("symbol", sa.String(length=32), nullable=False),
            sa.Column("trade_date", sa.Date(), nullable=False),
            sa.Column("open_price", sa.Numeric(18, 6), nullable=False),
            sa.Column("high_price", sa.Numeric(18, 6), nullable=False),
            sa.Column("low_price", sa.Numeric(18, 6), nullable=False),
            sa.Column("close_price", sa.Numeric(18, 6), nullable=False),
            sa.Column("volume", sa.Numeric(24, 4), nullable=False),
            sa.Column("source_dataset", sa.String(length=64), nullable=False, server_default="flatfile_day_aggs"),
            sa.Column("source_file_date", sa.Date(), nullable=False),
            sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.CheckConstraint("length(symbol) > 0", name="ck_historical_underlying_day_bars_symbol_not_empty"),
            sa.CheckConstraint("open_price > 0", name="ck_historical_underlying_day_bars_open_positive"),
            sa.CheckConstraint("high_price > 0", name="ck_historical_underlying_day_bars_high_positive"),
            sa.CheckConstraint("low_price > 0", name="ck_historical_underlying_day_bars_low_positive"),
            sa.CheckConstraint("close_price > 0", name="ck_historical_underlying_day_bars_close_positive"),
            sa.CheckConstraint("volume >= 0", name="ck_historical_underlying_day_bars_volume_nonneg"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("symbol", "trade_date", name="uq_historical_underlying_day_bars_symbol_date"),
        )
        op.create_index(
            "ix_historical_underlying_day_bars_symbol_date",
            "historical_underlying_day_bars",
            ["symbol", "trade_date"],
            unique=False,
        )
        _create_updated_at_trigger("historical_underlying_day_bars")

    if "historical_option_day_bars" not in tables:
        op.create_table(
            "historical_option_day_bars",
            sa.Column("id", GUID(), nullable=False),
            sa.Column("option_ticker", sa.String(length=64), nullable=False),
            sa.Column("underlying_symbol", sa.String(length=32), nullable=False),
            sa.Column("trade_date", sa.Date(), nullable=False),
            sa.Column("expiration_date", sa.Date(), nullable=False),
            sa.Column("contract_type", sa.String(length=8), nullable=False),
            sa.Column("strike_price", sa.Numeric(18, 4), nullable=False),
            sa.Column("open_price", sa.Numeric(18, 6), nullable=False),
            sa.Column("high_price", sa.Numeric(18, 6), nullable=False),
            sa.Column("low_price", sa.Numeric(18, 6), nullable=False),
            sa.Column("close_price", sa.Numeric(18, 6), nullable=False),
            sa.Column("volume", sa.Numeric(24, 4), nullable=False),
            sa.Column("source_dataset", sa.String(length=64), nullable=False, server_default="flatfile_day_aggs"),
            sa.Column("source_file_date", sa.Date(), nullable=False),
            sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.CheckConstraint("length(option_ticker) > 0", name="ck_historical_option_day_bars_ticker_not_empty"),
            sa.CheckConstraint("length(underlying_symbol) > 0", name="ck_historical_option_day_bars_symbol_not_empty"),
            sa.CheckConstraint("contract_type IN ('call', 'put')", name="ck_historical_option_day_bars_contract_type"),
            sa.CheckConstraint("strike_price > 0", name="ck_historical_option_day_bars_strike_positive"),
            sa.CheckConstraint("open_price >= 0", name="ck_historical_option_day_bars_open_nonneg"),
            sa.CheckConstraint("high_price >= 0", name="ck_historical_option_day_bars_high_nonneg"),
            sa.CheckConstraint("low_price >= 0", name="ck_historical_option_day_bars_low_nonneg"),
            sa.CheckConstraint("close_price >= 0", name="ck_historical_option_day_bars_close_nonneg"),
            sa.CheckConstraint("volume >= 0", name="ck_historical_option_day_bars_volume_nonneg"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("option_ticker", "trade_date", name="uq_historical_option_day_bars_ticker_date"),
        )
        op.create_index(
            "ix_historical_option_day_bars_underlying_date",
            "historical_option_day_bars",
            ["underlying_symbol", "trade_date"],
            unique=False,
        )
        op.create_index(
            "ix_historical_option_day_bars_lookup",
            "historical_option_day_bars",
            ["underlying_symbol", "trade_date", "contract_type", "expiration_date", "strike_price"],
            unique=False,
        )
        _create_updated_at_trigger("historical_option_day_bars")

    if "historical_ex_dividend_dates" not in tables:
        op.create_table(
            "historical_ex_dividend_dates",
            sa.Column("id", GUID(), nullable=False),
            sa.Column("symbol", sa.String(length=32), nullable=False),
            sa.Column("ex_dividend_date", sa.Date(), nullable=False),
            sa.Column("cash_amount", sa.Numeric(18, 6), nullable=True),
            sa.Column("source_dataset", sa.String(length=64), nullable=False, server_default="rest_dividends"),
            sa.Column("source_file_date", sa.Date(), nullable=False),
            sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.CheckConstraint("length(symbol) > 0", name="ck_historical_ex_dividend_dates_symbol_not_empty"),
            sa.CheckConstraint("cash_amount IS NULL OR cash_amount >= 0", name="ck_historical_ex_dividend_dates_cash_nonneg"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("symbol", "ex_dividend_date", name="uq_historical_ex_dividend_dates_symbol_date"),
        )
        op.create_index(
            "ix_historical_ex_dividend_dates_symbol_date",
            "historical_ex_dividend_dates",
            ["symbol", "ex_dividend_date"],
            unique=False,
        )
        _create_updated_at_trigger("historical_ex_dividend_dates")

    if "historical_treasury_yields" not in tables:
        op.create_table(
            "historical_treasury_yields",
            sa.Column("id", GUID(), nullable=False),
            sa.Column("trade_date", sa.Date(), nullable=False),
            sa.Column("yield_3_month", sa.Numeric(10, 6), nullable=False),
            sa.Column("source_dataset", sa.String(length=64), nullable=False, server_default="rest_treasury"),
            sa.Column("source_file_date", sa.Date(), nullable=False),
            sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.CheckConstraint("yield_3_month >= 0 AND yield_3_month <= 1", name="ck_historical_treasury_yields_3m_range"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("trade_date", name="uq_historical_treasury_yields_trade_date"),
        )
        op.create_index(
            "ix_historical_treasury_yields_trade_date",
            "historical_treasury_yields",
            ["trade_date"],
            unique=False,
        )
        _create_updated_at_trigger("historical_treasury_yields")

    if bind.dialect.name == "postgresql":
        op.execute("ALTER TABLE backtest_runs DROP CONSTRAINT IF EXISTS ck_backtest_runs_valid_data_source")
        op.execute(
            """
            ALTER TABLE backtest_runs
            ADD CONSTRAINT ck_backtest_runs_valid_data_source
            CHECK (data_source IN ('massive', 'manual', 'historical_flatfile'))
            """
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = set(inspector.get_table_names())

    if bind.dialect.name == "postgresql":
        op.execute("ALTER TABLE backtest_runs DROP CONSTRAINT IF EXISTS ck_backtest_runs_valid_data_source")
        op.execute(
            """
            ALTER TABLE backtest_runs
            ADD CONSTRAINT ck_backtest_runs_valid_data_source
            CHECK (data_source IN ('massive', 'manual'))
            """
        )

    for table_name, indexes in [
        ("historical_treasury_yields", ["ix_historical_treasury_yields_trade_date"]),
        ("historical_ex_dividend_dates", ["ix_historical_ex_dividend_dates_symbol_date"]),
        ("historical_option_day_bars", ["ix_historical_option_day_bars_lookup", "ix_historical_option_day_bars_underlying_date"]),
        ("historical_underlying_day_bars", ["ix_historical_underlying_day_bars_symbol_date"]),
    ]:
        if table_name not in tables:
            continue
        if bind.dialect.name == "postgresql":
            op.execute(f"DROP TRIGGER IF EXISTS trg_{table_name}_updated_at ON {table_name};")
        for index_name in indexes:
            op.drop_index(index_name, table_name=table_name)
        op.drop_table(table_name)
