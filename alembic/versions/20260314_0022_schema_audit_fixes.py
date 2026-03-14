"""Schema audit fixes: error_code on symbol_analyses, unique template name,
regime_labels text, backtest_runs updated_at, drop redundant daily_recs index.

Revision ID: 0022_schema_audit
Revises: 0021
Create Date: 2026-03-14
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260314_0022"
down_revision = "20260314_0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "symbol_analyses",
        sa.Column("error_code", sa.String(64), nullable=True),
    )

    op.create_unique_constraint(
        "uq_backtest_templates_user_name",
        "backtest_templates",
        ["user_id", "name"],
    )

    op.alter_column(
        "daily_recommendations",
        "regime_labels",
        type_=sa.Text(),
        existing_type=sa.String(255),
        existing_nullable=False,
    )

    op.add_column(
        "backtest_runs",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.execute(
        sa.text("UPDATE backtest_runs SET updated_at = created_at WHERE updated_at IS NULL")
    )

    op.drop_index("ix_daily_recs_pipeline_run_id", table_name="daily_recommendations")


def downgrade() -> None:
    op.create_index(
        "ix_daily_recs_pipeline_run_id",
        "daily_recommendations",
        ["pipeline_run_id"],
    )

    op.drop_column("backtest_runs", "updated_at")

    op.alter_column(
        "daily_recommendations",
        "regime_labels",
        type_=sa.String(255),
        existing_type=sa.Text(),
        existing_nullable=False,
    )

    op.drop_constraint(
        "uq_backtest_templates_user_name",
        "backtest_templates",
        type_="unique",
    )

    op.drop_column("symbol_analyses", "error_code")
