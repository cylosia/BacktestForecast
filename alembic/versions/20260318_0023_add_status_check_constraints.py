"""Add CHECK constraints for status columns and unique constraints for result ranking.

Ensures all job-status columns have a DB-level CHECK constraint limiting
them to their valid status values, and that (job_id, rank) is unique in
scanner_recommendations and sweep_results.

The constraints may already exist if the baseline migration created them;
every statement uses IF NOT EXISTS / existence checks to be idempotent.

Revision ID: 20260318_0023
Revises: 20260318_0022
Create Date: 2026-03-18
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260318_0023"
down_revision = "20260318_0022"
branch_labels = None
depends_on = None

_JOB_STATUS = "('queued', 'running', 'succeeded', 'failed', 'cancelled')"
_EXPORT_STATUS = "('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')"

_STATUS_CONSTRAINTS: list[tuple[str, str, str]] = [
    ("backtest_runs", "ck_backtest_runs_status_valid", f"status IN {_JOB_STATUS}"),
    ("scanner_jobs", "ck_scanner_jobs_status_valid", f"status IN {_JOB_STATUS}"),
    ("sweep_jobs", "ck_sweep_jobs_status_valid", f"status IN {_JOB_STATUS}"),
    ("export_jobs", "ck_export_jobs_status_valid", f"status IN {_EXPORT_STATUS}"),
]

_UNIQUE_CONSTRAINTS: list[tuple[str, str, list[str]]] = [
    ("scanner_recommendations", "uq_scanner_recommendations_job_rank", ["scanner_job_id", "rank"]),
    ("sweep_results", "uq_sweep_results_job_rank", ["sweep_job_id", "rank"]),
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
    for table, name, expr in _STATUS_CONSTRAINTS:
        if not _constraint_exists(table, name):
            op.execute(sa.text(
                f"ALTER TABLE {table} "
                f"ADD CONSTRAINT {name} CHECK ({expr}) NOT VALID"
            ))
            op.execute(sa.text(
                f"ALTER TABLE {table} VALIDATE CONSTRAINT {name}"
            ))

    for table, name, columns in _UNIQUE_CONSTRAINTS:
        if not _constraint_exists(table, name):
            op.create_unique_constraint(name, table, columns)


def downgrade() -> None:
    for table, name, _columns in reversed(_UNIQUE_CONSTRAINTS):
        if _constraint_exists(table, name):
            op.drop_constraint(name, table)

    for table, name, _expr in reversed(_STATUS_CONSTRAINTS):
        if _constraint_exists(table, name):
            op.drop_constraint(name, table)
