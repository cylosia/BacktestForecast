"""Add durable option contract catalog snapshots.

Revision ID: 20260327_0007
Revises: 20260326_0006
Create Date: 2026-03-27 13:40:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

from backtestforecast.db.types import GUID, JSON_DEFAULT_EMPTY_ARRAY, JSON_VARIANT

revision = "20260327_0007"
down_revision = "20260326_0006"
branch_labels = None
depends_on = None

_TABLE_NAME = "option_contract_catalog_snapshots"


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if _TABLE_NAME not in inspector.get_table_names():
        op.create_table(
            _TABLE_NAME,
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
            sa.CheckConstraint("length(symbol) > 0", name="ck_option_contract_catalog_snapshots_symbol_not_empty"),
            sa.CheckConstraint(
                "contract_type IN ('call', 'put')",
                name="ck_option_contract_catalog_snapshots_contract_type",
            ),
            sa.CheckConstraint(
                "strike_price_gte IS NULL OR strike_price_gte >= 0",
                name="ck_option_contract_catalog_snapshots_strike_gte_nonneg",
            ),
            sa.CheckConstraint(
                "strike_price_lte IS NULL OR strike_price_lte >= 0",
                name="ck_option_contract_catalog_snapshots_strike_lte_nonneg",
            ),
            sa.CheckConstraint(
                "strike_price_gte IS NULL OR strike_price_lte IS NULL OR strike_price_gte <= strike_price_lte",
                name="ck_option_contract_catalog_snapshots_strike_bounds",
            ),
            sa.CheckConstraint(
                "contract_count >= 0",
                name="ck_option_contract_catalog_snapshots_contract_count_nonneg",
            ),
            sa.PrimaryKeyConstraint("id", name=op.f("pk_option_contract_catalog_snapshots")),
            sa.UniqueConstraint(
                "symbol",
                "as_of_date",
                "contract_type",
                "expiration_date",
                "strike_price_gte",
                "strike_price_lte",
                name="uq_option_contract_catalog_snapshots_query",
            ),
        )
    existing_indexes = {index["name"] for index in inspector.get_indexes(_TABLE_NAME)}
    if "ix_option_contract_catalog_snapshots_lookup" not in existing_indexes:
        op.create_index(
            "ix_option_contract_catalog_snapshots_lookup",
            _TABLE_NAME,
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
        op.execute(f"DROP TRIGGER IF EXISTS trg_{_TABLE_NAME}_updated_at ON {_TABLE_NAME};")
        op.execute(
            f"""
            CREATE TRIGGER trg_{_TABLE_NAME}_updated_at
            BEFORE UPDATE ON {_TABLE_NAME}
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
            """
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if _TABLE_NAME not in inspector.get_table_names():
        return
    if bind.dialect.name == "postgresql":
        op.execute(f"DROP TRIGGER IF EXISTS trg_{_TABLE_NAME}_updated_at ON {_TABLE_NAME};")
    op.drop_index("ix_option_contract_catalog_snapshots_lookup", table_name=_TABLE_NAME)
    op.drop_table(_TABLE_NAME)
