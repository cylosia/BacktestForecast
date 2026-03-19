"""Drop duplicate status CHECK constraints.

Baseline (0001) and migration 0023 both created status CHECK constraints
on 4 tables with different names but identical expressions.  This removes
the 0023 duplicates since the baseline originals are authoritative.

Revision ID: 20260319_0026
Revises: 20260318_0026
Create Date: 2026-03-19
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260319_0026"
down_revision = "20260318_0026"
branch_labels = None
depends_on = None

_JOB_STATUS = "('queued', 'running', 'succeeded', 'failed', 'cancelled')"
_EXPORT_STATUS = "('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')"

_DUPLICATES: list[tuple[str, str]] = [
    ("backtest_runs", "ck_backtest_runs_status_valid"),
    ("scanner_jobs", "ck_scanner_jobs_status_valid"),
    ("sweep_jobs", "ck_sweep_jobs_status_valid"),
    ("export_jobs", "ck_export_jobs_status_valid"),
]


def _constraint_exists(table: str, name: str) -> bool:
    bind = op.get_bind()
    row = bind.execute(
        sa.text(
            "SELECT 1 FROM information_schema.table_constraints "
            "WHERE table_name = :tbl AND constraint_name = :name"
        ),
        {"tbl": table, "name": name},
    ).fetchone()
    return row is not None


def upgrade() -> None:
    for table, name in _DUPLICATES:
        if _constraint_exists(table, name):
            op.drop_constraint(name, table, type_="check")


def downgrade() -> None:
    constraints: list[tuple[str, str, str]] = [
        ("backtest_runs", "ck_backtest_runs_status_valid", f"status IN {_JOB_STATUS}"),
        ("scanner_jobs", "ck_scanner_jobs_status_valid", f"status IN {_JOB_STATUS}"),
        ("sweep_jobs", "ck_sweep_jobs_status_valid", f"status IN {_JOB_STATUS}"),
        ("export_jobs", "ck_export_jobs_status_valid", f"status IN {_EXPORT_STATUS}"),
    ]
    for table, name, expr in constraints:
        if not _constraint_exists(table, name):
            op.execute(sa.text(
                f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expr}) NOT VALID"
            ))
            op.execute(sa.text(
                f"ALTER TABLE {table} VALIDATE CONSTRAINT {name}"
            ))
