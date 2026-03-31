"""expand historical dividend metadata

Revision ID: 20260330_0012
Revises: 20260330_0011
Create Date: 2026-03-30 16:50:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260330_0012"
down_revision = "20260330_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("historical_ex_dividend_dates", schema=None) as batch_op:
        batch_op.add_column(sa.Column("provider_dividend_id", sa.String(length=128), nullable=True))
        batch_op.add_column(sa.Column("currency", sa.String(length=8), nullable=True))
        batch_op.add_column(sa.Column("declaration_date", sa.Date(), nullable=True))
        batch_op.add_column(sa.Column("record_date", sa.Date(), nullable=True))
        batch_op.add_column(sa.Column("pay_date", sa.Date(), nullable=True))
        batch_op.add_column(sa.Column("frequency", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("distribution_type", sa.String(length=32), nullable=True))
        batch_op.add_column(sa.Column("historical_adjustment_factor", sa.Numeric(18, 10), nullable=True))
        batch_op.add_column(sa.Column("split_adjusted_cash_amount", sa.Numeric(18, 6), nullable=True))
        batch_op.create_check_constraint(
            "ck_historical_ex_dividend_dates_split_cash_nonneg",
            "split_adjusted_cash_amount IS NULL OR split_adjusted_cash_amount >= 0",
        )
        batch_op.create_check_constraint(
            "ck_historical_ex_dividend_dates_adjustment_factor_nonneg",
            "historical_adjustment_factor IS NULL OR historical_adjustment_factor >= 0",
        )
        batch_op.create_check_constraint(
            "ck_historical_ex_dividend_dates_frequency_nonneg",
            "frequency IS NULL OR frequency >= 0",
        )
        batch_op.create_check_constraint(
            "ck_historical_ex_dividend_dates_distribution_type",
            "distribution_type IS NULL OR distribution_type IN ('recurring', 'special', 'supplemental', 'irregular', 'unknown')",
        )


def downgrade() -> None:
    with op.batch_alter_table("historical_ex_dividend_dates", schema=None) as batch_op:
        batch_op.drop_constraint("ck_historical_ex_dividend_dates_distribution_type", type_="check")
        batch_op.drop_constraint("ck_historical_ex_dividend_dates_frequency_nonneg", type_="check")
        batch_op.drop_constraint("ck_historical_ex_dividend_dates_adjustment_factor_nonneg", type_="check")
        batch_op.drop_constraint("ck_historical_ex_dividend_dates_split_cash_nonneg", type_="check")
        batch_op.drop_column("split_adjusted_cash_amount")
        batch_op.drop_column("historical_adjustment_factor")
        batch_op.drop_column("distribution_type")
        batch_op.drop_column("frequency")
        batch_op.drop_column("pay_date")
        batch_op.drop_column("record_date")
        batch_op.drop_column("declaration_date")
        batch_op.drop_column("currency")
        batch_op.drop_column("provider_dividend_id")
