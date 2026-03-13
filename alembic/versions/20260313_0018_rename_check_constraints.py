"""Rename check constraints to match ORM naming convention.

Migration 0017 created check constraints with bare names (e.g.
``valid_plan_tier``), but the ORM's naming convention expects them to be
``ck_<table>_<constraint_name>``.  Running ``alembic revision --autogenerate``
would generate a destructive drop+re-create migration.  This migration
renames them in-place so the DB and ORM agree.

Revision ID: 20260313_0018
Revises: 20260313_0017
Create Date: 2026-03-13

"""
from __future__ import annotations

from alembic import op

revision = "20260313_0018"
down_revision = "20260313_0017"
branch_labels = None
depends_on = None

_RENAMES = [
    ("users", "valid_plan_tier", "ck_users_valid_plan_tier"),
    ("backtest_runs", "valid_run_status", "ck_backtest_runs_valid_run_status"),
    ("scanner_jobs", "valid_job_status", "ck_scanner_jobs_valid_job_status"),
    ("export_jobs", "valid_export_status", "ck_export_jobs_valid_export_status"),
    ("nightly_pipeline_runs", "valid_pipeline_status", "ck_nightly_pipeline_runs_valid_pipeline_status"),
    ("symbol_analyses", "valid_analysis_status", "ck_symbol_analyses_valid_analysis_status"),
]


def upgrade() -> None:
    for table, old_name, new_name in _RENAMES:
        op.execute(
            f"ALTER TABLE {table} RENAME CONSTRAINT {old_name} TO {new_name}"
        )


def downgrade() -> None:
    for table, old_name, new_name in _RENAMES:
        op.execute(
            f"ALTER TABLE {table} RENAME CONSTRAINT {new_name} TO {old_name}"
        )
