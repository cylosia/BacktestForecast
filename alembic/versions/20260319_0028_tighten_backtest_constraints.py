"""Tighten backtest_runs CHECK constraints to match Pydantic schema limits.

- target_dte: 730 -> 365
- dte_tolerance_days: 120 -> 60
- max_holding_days: 365 -> 120
- commission_per_contract: 1000 -> 100
- account_size: 1000000000 -> 100000000

Revision ID: 20260319_0028
Revises: 20260319_0027
Create Date: 2026-03-19
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260319_0028"
down_revision = "20260319_0027"
branch_labels = None
depends_on = None

_NEW = [
    ("backtest_runs", "ck_backtest_runs_target_dte_range", "target_dte >= 1 AND target_dte <= 365"),
    ("backtest_runs", "ck_backtest_runs_dte_tolerance_range", "dte_tolerance_days >= 0 AND dte_tolerance_days <= 60"),
    ("backtest_runs", "ck_backtest_runs_holding_days_range", "max_holding_days >= 1 AND max_holding_days <= 120"),
    ("backtest_runs", "ck_backtest_runs_commission_max", "commission_per_contract <= 100"),
    ("backtest_runs", "ck_backtest_runs_account_size_max", "account_size <= 100000000"),
]

_OLD = [
    ("backtest_runs", "ck_backtest_runs_target_dte_range", "target_dte >= 1 AND target_dte <= 730"),
    ("backtest_runs", "ck_backtest_runs_dte_tolerance_range", "dte_tolerance_days >= 0 AND dte_tolerance_days <= 120"),
    ("backtest_runs", "ck_backtest_runs_holding_days_range", "max_holding_days >= 1 AND max_holding_days <= 365"),
    ("backtest_runs", "ck_backtest_runs_commission_max", "commission_per_contract <= 1000"),
    ("backtest_runs", "ck_backtest_runs_account_size_max", "account_size <= 1000000000"),
]


def upgrade() -> None:
    for table, name, expr in _NEW:
        op.execute(sa.text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}"))
        op.execute(sa.text(
            f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expr}) NOT VALID"
        ))
        op.execute(sa.text(f"ALTER TABLE {table} VALIDATE CONSTRAINT {name}"))


def downgrade() -> None:
    for table, name, expr in _OLD:
        op.execute(sa.text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}"))
        op.execute(sa.text(
            f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expr}) NOT VALID"
        ))
        op.execute(sa.text(f"ALTER TABLE {table} VALIDATE CONSTRAINT {name}"))
