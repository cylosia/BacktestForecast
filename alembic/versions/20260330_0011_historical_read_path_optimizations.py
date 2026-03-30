"""Optimize historical backtest read paths.

Revision ID: 20260330_0011
Revises: 20260328_0010
Create Date: 2026-03-30 18:15:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

from backtestforecast.db.types import GUID, JSON_DEFAULT_EMPTY_ARRAY, JSON_VARIANT

revision = "20260330_0011"
down_revision = "20260328_0010"
branch_labels = None
depends_on = None

_HISTORICAL_CATALOG_TABLE = "historical_option_contract_catalog_snapshots"
_UNDERLYING_TABLE = "historical_underlying_day_bars"
_OPTION_TABLE = "historical_option_day_bars"
_UNDERLYING_COVERING_INDEX = "ix_historical_underlying_day_bars_covering"
_OPTION_CONTRACT_PROJECTION_INDEX = "ix_historical_option_day_bars_contract_projection"
_OPTION_QUOTE_PROJECTION_INDEX = "ix_historical_option_day_bars_quote_projection"
_HISTORICAL_CATALOG_LOOKUP_INDEX = "ix_historical_option_contract_catalog_snapshots_lookup"


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if _HISTORICAL_CATALOG_TABLE not in inspector.get_table_names():
        op.create_table(
            _HISTORICAL_CATALOG_TABLE,
            sa.Column("id", GUID(), nullable=False),
            sa.Column("symbol", sa.String(length=32), nullable=False),
            sa.Column("as_of_date", sa.Date(), nullable=False),
            sa.Column("contract_type", sa.String(length=8), nullable=False),
            sa.Column("expiration_date", sa.Date(), nullable=False),
            sa.Column("strike_price_gte", sa.Numeric(18, 4), nullable=True),
            sa.Column("strike_price_lte", sa.Numeric(18, 4), nullable=True),
            sa.Column("contracts_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_ARRAY),
            sa.Column("contract_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.CheckConstraint(
                "length(symbol) > 0",
                name="ck_historical_option_contract_catalog_snapshots_symbol_not_empty",
            ),
            sa.CheckConstraint(
                "contract_type IN ('call', 'put')",
                name="ck_historical_option_contract_catalog_snapshots_contract_type",
            ),
            sa.CheckConstraint(
                "strike_price_gte IS NULL OR strike_price_gte >= 0",
                name="ck_historical_option_contract_catalog_snapshots_strike_gte_nonneg",
            ),
            sa.CheckConstraint(
                "strike_price_lte IS NULL OR strike_price_lte >= 0",
                name="ck_historical_option_contract_catalog_snapshots_strike_lte_nonneg",
            ),
            sa.CheckConstraint(
                "strike_price_gte IS NULL OR strike_price_lte IS NULL OR strike_price_gte <= strike_price_lte",
                name="ck_historical_option_contract_catalog_snapshots_strike_bounds",
            ),
            sa.CheckConstraint(
                "contract_count >= 0",
                name="ck_historical_option_contract_catalog_snapshots_contract_count_nonneg",
            ),
            sa.PrimaryKeyConstraint("id", name=op.f("pk_historical_option_contract_catalog_snapshots")),
            sa.UniqueConstraint(
                "symbol",
                "as_of_date",
                "contract_type",
                "expiration_date",
                "strike_price_gte",
                "strike_price_lte",
                name="uq_historical_option_contract_catalog_snapshots_query",
            ),
        )

    existing_catalog_indexes = {index["name"] for index in inspector.get_indexes(_HISTORICAL_CATALOG_TABLE)}
    if _HISTORICAL_CATALOG_LOOKUP_INDEX not in existing_catalog_indexes:
        op.create_index(
            _HISTORICAL_CATALOG_LOOKUP_INDEX,
            _HISTORICAL_CATALOG_TABLE,
            ["symbol", "as_of_date", "contract_type", "expiration_date"],
            unique=False,
        )

    if bind.dialect.name == "postgresql":
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
        op.execute(f"DROP TRIGGER IF EXISTS trg_{_HISTORICAL_CATALOG_TABLE}_updated_at ON {_HISTORICAL_CATALOG_TABLE};")
        op.execute(
            f"""
            CREATE TRIGGER trg_{_HISTORICAL_CATALOG_TABLE}_updated_at
            BEFORE UPDATE ON {_HISTORICAL_CATALOG_TABLE}
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
            """
        )
        op.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {_UNDERLYING_COVERING_INDEX}
            ON {_UNDERLYING_TABLE} (symbol, trade_date)
            INCLUDE (open_price, high_price, low_price, close_price, volume);
            """
        )
        op.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {_OPTION_CONTRACT_PROJECTION_INDEX}
            ON {_OPTION_TABLE} (underlying_symbol, trade_date, contract_type, expiration_date, strike_price)
            INCLUDE (option_ticker);
            """
        )
        op.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {_OPTION_QUOTE_PROJECTION_INDEX}
            ON {_OPTION_TABLE} (option_ticker, trade_date)
            INCLUDE (close_price);
            """
        )
    else:
        option_indexes = {index["name"] for index in inspector.get_indexes(_OPTION_TABLE)}
        underlying_indexes = {index["name"] for index in inspector.get_indexes(_UNDERLYING_TABLE)}
        if _UNDERLYING_COVERING_INDEX not in underlying_indexes:
            op.create_index(
                _UNDERLYING_COVERING_INDEX,
                _UNDERLYING_TABLE,
                ["symbol", "trade_date"],
                unique=False,
            )
        if _OPTION_CONTRACT_PROJECTION_INDEX not in option_indexes:
            op.create_index(
                _OPTION_CONTRACT_PROJECTION_INDEX,
                _OPTION_TABLE,
                ["underlying_symbol", "trade_date", "contract_type", "expiration_date", "strike_price"],
                unique=False,
            )
        if _OPTION_QUOTE_PROJECTION_INDEX not in option_indexes:
            op.create_index(
                _OPTION_QUOTE_PROJECTION_INDEX,
                _OPTION_TABLE,
                ["option_ticker", "trade_date"],
                unique=False,
            )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if bind.dialect.name == "postgresql":
        op.execute(f"DROP INDEX IF EXISTS {_OPTION_QUOTE_PROJECTION_INDEX};")
        op.execute(f"DROP INDEX IF EXISTS {_OPTION_CONTRACT_PROJECTION_INDEX};")
        op.execute(f"DROP INDEX IF EXISTS {_UNDERLYING_COVERING_INDEX};")
        op.execute(f"DROP TRIGGER IF EXISTS trg_{_HISTORICAL_CATALOG_TABLE}_updated_at ON {_HISTORICAL_CATALOG_TABLE};")
    else:
        option_indexes = {index["name"] for index in inspector.get_indexes(_OPTION_TABLE)}
        underlying_indexes = {index["name"] for index in inspector.get_indexes(_UNDERLYING_TABLE)}
        if _OPTION_QUOTE_PROJECTION_INDEX in option_indexes:
            op.drop_index(_OPTION_QUOTE_PROJECTION_INDEX, table_name=_OPTION_TABLE)
        if _OPTION_CONTRACT_PROJECTION_INDEX in option_indexes:
            op.drop_index(_OPTION_CONTRACT_PROJECTION_INDEX, table_name=_OPTION_TABLE)
        if _UNDERLYING_COVERING_INDEX in underlying_indexes:
            op.drop_index(_UNDERLYING_COVERING_INDEX, table_name=_UNDERLYING_TABLE)

    if _HISTORICAL_CATALOG_TABLE not in inspector.get_table_names():
        return
    existing_catalog_indexes = {index["name"] for index in inspector.get_indexes(_HISTORICAL_CATALOG_TABLE)}
    if _HISTORICAL_CATALOG_LOOKUP_INDEX in existing_catalog_indexes:
        op.drop_index(_HISTORICAL_CATALOG_LOOKUP_INDEX, table_name=_HISTORICAL_CATALOG_TABLE)
    op.drop_table(_HISTORICAL_CATALOG_TABLE)
