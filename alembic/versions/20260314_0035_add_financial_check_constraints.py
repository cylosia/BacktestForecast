"""Add CHECK constraints on financial columns.

Revision ID: 20260314_0035
Revises: 20260314_0033
Create Date: 2026-03-14
"""
from __future__ import annotations

from alembic import op

revision = "20260314_0035"
down_revision = "20260314_0033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE backtest_runs
        ADD CONSTRAINT ck_backtest_runs_account_positive
        CHECK (account_size > 0) NOT VALID
    """)
    op.execute("""
        ALTER TABLE backtest_runs
        VALIDATE CONSTRAINT ck_backtest_runs_account_positive
    """)
    op.execute("""
        ALTER TABLE backtest_runs
        ADD CONSTRAINT ck_backtest_runs_risk_pct_range
        CHECK (risk_per_trade_pct > 0 AND risk_per_trade_pct <= 100) NOT VALID
    """)
    op.execute("""
        ALTER TABLE backtest_runs
        VALIDATE CONSTRAINT ck_backtest_runs_risk_pct_range
    """)
    op.execute("""
        ALTER TABLE backtest_runs
        ADD CONSTRAINT ck_backtest_runs_commission_nonneg
        CHECK (commission_per_contract >= 0) NOT VALID
    """)
    op.execute("""
        ALTER TABLE backtest_runs
        VALIDATE CONSTRAINT ck_backtest_runs_commission_nonneg
    """)
    op.execute("""
        ALTER TABLE backtest_runs
        ADD CONSTRAINT ck_backtest_runs_date_order
        CHECK (date_from < date_to) NOT VALID
    """)
    op.execute("""
        ALTER TABLE backtest_runs
        VALIDATE CONSTRAINT ck_backtest_runs_date_order
    """)
    op.execute("""
        ALTER TABLE backtest_runs
        ADD CONSTRAINT ck_backtest_runs_holding_days_positive
        CHECK (max_holding_days >= 1) NOT VALID
    """)
    op.execute("""
        ALTER TABLE backtest_runs
        VALIDATE CONSTRAINT ck_backtest_runs_holding_days_positive
    """)


def downgrade() -> None:
    op.drop_constraint("ck_backtest_runs_holding_days_positive", "backtest_runs", type_="check")
    op.drop_constraint("ck_backtest_runs_date_order", "backtest_runs", type_="check")
    op.drop_constraint("ck_backtest_runs_commission_nonneg", "backtest_runs", type_="check")
    op.drop_constraint("ck_backtest_runs_risk_pct_range", "backtest_runs", type_="check")
    op.drop_constraint("ck_backtest_runs_account_positive", "backtest_runs", type_="check")
