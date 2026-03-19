"""Add upper-bound CHECK constraints on backtest_runs numeric fields.

Revision ID: 20260318_0025
Revises: 20260318_0024
Create Date: 2026-03-18
"""
import sqlalchemy as sa
from alembic import op

revision = "20260318_0025"
down_revision = "20260318_0024"
branch_labels = None
depends_on = None

_CONSTRAINTS = [
    ("backtest_runs", "ck_backtest_runs_holding_days_range", "max_holding_days >= 1 AND max_holding_days <= 365"),
    ("backtest_runs", "ck_backtest_runs_target_dte_range", "target_dte >= 1 AND target_dte <= 730"),
    ("backtest_runs", "ck_backtest_runs_dte_tolerance_range", "dte_tolerance_days >= 0 AND dte_tolerance_days <= 120"),
    ("backtest_runs", "ck_backtest_runs_account_size_max", "account_size <= 1000000000"),
    ("backtest_runs", "ck_backtest_runs_commission_max", "commission_per_contract <= 1000"),
    ("scanner_jobs", "ck_scanner_jobs_refresh_priority_range", "refresh_priority >= 0 AND refresh_priority <= 100"),
]

_OLD_CONSTRAINTS = [
    ("backtest_runs", "ck_backtest_runs_holding_days_positive"),
    ("backtest_runs", "ck_backtest_runs_target_dte_nonneg"),
    ("backtest_runs", "ck_backtest_runs_dte_tolerance_nonneg"),
]


def upgrade() -> None:
    for table, name in _OLD_CONSTRAINTS:
        op.execute(sa.text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}"))
    for table, name, expr in _CONSTRAINTS:
        op.execute(sa.text(
            f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expr}) NOT VALID"
        ))
    for table, name, _ in _CONSTRAINTS:
        op.execute(sa.text(f"ALTER TABLE {table} VALIDATE CONSTRAINT {name}"))


def downgrade() -> None:
    for table, name, _ in _CONSTRAINTS:
        op.execute(sa.text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}"))
    op.execute(sa.text("ALTER TABLE backtest_runs ADD CONSTRAINT ck_backtest_runs_holding_days_positive CHECK (max_holding_days >= 1)"))
    op.execute(sa.text("ALTER TABLE backtest_runs ADD CONSTRAINT ck_backtest_runs_target_dte_nonneg CHECK (target_dte >= 0)"))
    op.execute(sa.text("ALTER TABLE backtest_runs ADD CONSTRAINT ck_backtest_runs_dte_tolerance_nonneg CHECK (dte_tolerance_days >= 0)"))
