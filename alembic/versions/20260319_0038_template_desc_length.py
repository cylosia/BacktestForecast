"""Constrain template description length to 2000 chars.

Revision ID: 20260319_0038
Revises: 20260319_0037
"""
from alembic import op
import sqlalchemy as sa


revision = "20260319_0038"
down_revision = "20260319_0037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "backtest_templates",
        "description",
        existing_type=sa.Text(),
        type_=sa.String(2000),
        existing_nullable=True,
    )
    op.create_check_constraint(
        "ck_backtest_templates_desc_length",
        "backtest_templates",
        "description IS NULL OR length(description) <= 2000",
    )


def downgrade() -> None:
    op.drop_constraint("ck_backtest_templates_desc_length", "backtest_templates", type_="check")
    op.alter_column(
        "backtest_templates",
        "description",
        existing_type=sa.String(2000),
        type_=sa.Text(),
        existing_nullable=True,
    )
