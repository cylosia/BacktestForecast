"""Tighten backtest_runs CHECK constraints to match Pydantic schema limits.

- target_dte: 730 -> 365
- dte_tolerance_days: 120 -> 60
- max_holding_days: 365 -> 120
- commission_per_contract: 1000 -> 100
- account_size: 1000000000 -> 100000000

Revision ID: 20260319_0029
Revises: 20260319_0028
Create Date: 2026-03-19
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260319_0029"
down_revision = "20260319_0028"
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
    bind = op.get_bind()
    row = bind.execute(sa.text(
        "SELECT count(*) FROM backtest_runs "
        "WHERE target_dte > 365 OR dte_tolerance_days > 60 "
        "OR max_holding_days > 120 OR commission_per_contract > 100 "
        "OR account_size > 100000000"
    )).scalar()
    if row and row > 0:
        import json, sys
        audit_rows = bind.execute(sa.text(
            "SELECT id, target_dte, dte_tolerance_days, max_holding_days, "
            "commission_per_contract, account_size FROM backtest_runs "
            "WHERE target_dte > 365 OR dte_tolerance_days > 60 "
            "OR max_holding_days > 120 OR commission_per_contract > 100 "
            "OR account_size > 100000000"
        )).fetchall()
        print(
            f"[migration 0029] Clipping {len(audit_rows)} rows. Original values:",
            file=sys.stderr,
        )
        for r in audit_rows:
            print(
                json.dumps({"id": str(r[0]), "target_dte": r[1], "dte_tolerance_days": r[2],
                            "max_holding_days": r[3], "commission": float(r[4]), "account_size": float(r[5])}),
                file=sys.stderr,
            )
        bind.execute(sa.text(
            "UPDATE backtest_runs SET "
            "target_dte = LEAST(target_dte, 365), "
            "dte_tolerance_days = LEAST(dte_tolerance_days, 60), "
            "max_holding_days = LEAST(max_holding_days, 120), "
            "commission_per_contract = LEAST(commission_per_contract, 100), "
            "account_size = LEAST(account_size, 100000000) "
            "WHERE target_dte > 365 OR dte_tolerance_days > 60 "
            "OR max_holding_days > 120 OR commission_per_contract > 100 "
            "OR account_size > 100000000"
        ))

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
