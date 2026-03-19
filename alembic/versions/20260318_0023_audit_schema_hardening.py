"""Audit schema hardening: status CHECKs, unique constraints, outbox defaults, email CHECK.

Consolidates three previously duplicate 0023 migrations:
- add_status_check_constraints (status CHECKs + result unique constraints)
- outbox_server_defaults (outbox_messages.task_kwargs_json default)
- audit_schema_hardening (sweep mode validation, outbox index, email CHECK)

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


def _index_exists(name: str) -> bool:
    bind = op.get_bind()
    result = bind.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE indexname = :name"),
        {"name": name},
    )
    return result.fetchone() is not None


def upgrade() -> None:
    # --- Status CHECK constraints (NOT VALID only — validation is deferred to post-deploy) ---
    for table, name, expr in _STATUS_CONSTRAINTS:
        if not _constraint_exists(table, name):
            op.execute(sa.text(
                f"ALTER TABLE {table} "
                f"ADD CONSTRAINT {name} CHECK ({expr}) NOT VALID"
            ))

    # --- Unique constraints on (job_id, rank) ---
    for table, name, columns in _UNIQUE_CONSTRAINTS:
        if not _constraint_exists(table, name):
            op.create_unique_constraint(name, table, columns)

    # --- Outbox server default ---
    op.alter_column(
        "outbox_messages",
        "task_kwargs_json",
        server_default=sa.text("'{}'::jsonb"),
    )

    # --- Outbox correlation_id index ---
    if not _index_exists("ix_outbox_messages_correlation_id"):
        op.create_index(
            "ix_outbox_messages_correlation_id",
            "outbox_messages",
            ["correlation_id"],
            unique=False,
        )

    # --- Email non-empty CHECK (NOT VALID only — validation is deferred to post-deploy) ---
    if not _constraint_exists("users", "ck_users_email_not_empty"):
        op.execute(sa.text(
            "ALTER TABLE users "
            "ADD CONSTRAINT ck_users_email_not_empty "
            "CHECK (email IS NULL OR length(email) > 0) NOT VALID"
        ))


def validate_constraints_post_deploy() -> None:
    """Validate NOT VALID constraints added by upgrade().

    Run manually after the deploy is stable and traffic is confirmed healthy:
        python -c "from alembic.versions.20260318_0023_audit_schema_hardening import validate_constraints_post_deploy; validate_constraints_post_deploy()"

    Each VALIDATE CONSTRAINT acquires a SHARE UPDATE EXCLUSIVE lock, which
    blocks only other schema changes but NOT reads or writes. However, on
    very large tables validation may take minutes, so prefer running during
    low-traffic windows.
    """
    for table, name, _expr in _STATUS_CONSTRAINTS:
        if _constraint_exists(table, name):
            op.execute(sa.text(
                f"ALTER TABLE {table} VALIDATE CONSTRAINT {name}"
            ))

    if _constraint_exists("sweep_jobs", "ck_sweep_jobs_valid_mode"):
        op.execute(sa.text(
            "ALTER TABLE sweep_jobs VALIDATE CONSTRAINT ck_sweep_jobs_valid_mode"
        ))

    if _constraint_exists("users", "ck_users_email_not_empty"):
        op.execute(sa.text(
            "ALTER TABLE users VALIDATE CONSTRAINT ck_users_email_not_empty"
        ))


def downgrade() -> None:
    op.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS ck_users_email_not_empty")

    if _index_exists("ix_outbox_messages_correlation_id"):
        op.drop_index("ix_outbox_messages_correlation_id", table_name="outbox_messages")

    op.alter_column(
        "outbox_messages",
        "task_kwargs_json",
        server_default=None,
    )

    for table, name, _columns in reversed(_UNIQUE_CONSTRAINTS):
        if _constraint_exists(table, name):
            op.drop_constraint(name, table)

    for table, name, _expr in reversed(_STATUS_CONSTRAINTS):
        if _constraint_exists(table, name):
            op.drop_constraint(name, table)
