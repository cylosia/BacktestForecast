"""Add CHECK constraints on financial columns.

Revision ID: 20260314_0033
Revises: 20260314_0032
Create Date: 2026-03-14
"""
from __future__ import annotations

from alembic import op

revision = "20260314_0033"
down_revision = "20260314_0032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_backtest_runs_account_positive",
        "backtest_runs",
        "account_size > 0",
    )
    op.create_check_constraint(
        "ck_backtest_runs_risk_pct_range",
        "backtest_runs",
        "risk_per_trade_pct > 0 AND risk_per_trade_pct <= 100",
    )
    op.create_check_constraint(
        "ck_backtest_runs_commission_nonneg",
        "backtest_runs",
        "commission_per_contract >= 0",
    )
    op.create_check_constraint(
        "ck_backtest_runs_date_order",
        "backtest_runs",
        "date_from < date_to",
    )
    op.create_check_constraint(
        "ck_backtest_runs_holding_days_positive",
        "backtest_runs",
        "max_holding_days >= 1",
    )


def downgrade() -> None:
    op.drop_constraint("ck_backtest_runs_holding_days_positive", "backtest_runs", type_="check")
    op.drop_constraint("ck_backtest_runs_date_order", "backtest_runs", type_="check")
    op.drop_constraint("ck_backtest_runs_commission_nonneg", "backtest_runs", type_="check")
    op.drop_constraint("ck_backtest_runs_risk_pct_range", "backtest_runs", type_="check")
    op.drop_constraint("ck_backtest_runs_account_positive", "backtest_runs", type_="check")
