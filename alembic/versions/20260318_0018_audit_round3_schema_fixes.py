"""Audit round 3 schema fixes.

- Add NightlyPipelineRun celery_task_id indexes
- Add NightlyPipelineRun partial queued index
- Add NightlyPipelineRun non-negative CHECK constraints (6 counters)
- Add ScannerJob engine_version CHECK constraint
- Add SweepJob engine_version CHECK constraint
- Fix StripeEvent idempotency_status server_default to 'processing'
- Add daily_recommendations updated_at trigger

Revision ID: 20260318_0018
Revises: 20260318_0017
Create Date: 2026-03-18
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260318_0018"
down_revision = "20260318_0017"
branch_labels = None
depends_on = None


def _check_exists(conn, table: str, constraint: str) -> bool:
    result = conn.execute(sa.text(
        "SELECT 1 FROM information_schema.table_constraints "
        "WHERE table_name = :t AND constraint_name = :c"
    ), {"t": table, "c": constraint})
    return result.fetchone() is not None


def _index_exists(conn, index_name: str) -> bool:
    result = conn.execute(sa.text(
        "SELECT 1 FROM pg_indexes WHERE indexname = :n"
    ), {"n": index_name})
    return result.fetchone() is not None


def upgrade() -> None:
    conn = op.get_bind()

    # -- NightlyPipelineRun celery_task_id indexes -------------------------
    if not _index_exists(conn, "ix_nightly_pipeline_runs_celery_task_id"):
        op.create_index(
            "ix_nightly_pipeline_runs_celery_task_id",
            "nightly_pipeline_runs",
            ["celery_task_id"],
        )
    if not _index_exists(conn, "ix_nightly_pipeline_runs_status_celery_created"):
        op.create_index(
            "ix_nightly_pipeline_runs_status_celery_created",
            "nightly_pipeline_runs",
            ["status", "celery_task_id", "created_at"],
        )

    # -- NightlyPipelineRun partial queued index ---------------------------
    if not _index_exists(conn, "ix_nightly_pipeline_runs_queued"):
        op.create_index(
            "ix_nightly_pipeline_runs_queued",
            "nightly_pipeline_runs",
            ["created_at"],
            postgresql_where=sa.text("status = 'queued'"),
        )

    # -- NightlyPipelineRun non-negative CHECK constraints -----------------
    _pipeline_checks = [
        ("ck_nightly_pipeline_runs_symbols_screened_nonneg", "symbols_screened >= 0"),
        ("ck_nightly_pipeline_runs_symbols_after_nonneg", "symbols_after_screen >= 0"),
        ("ck_nightly_pipeline_runs_pairs_nonneg", "pairs_generated >= 0"),
        ("ck_nightly_pipeline_runs_quick_bt_nonneg", "quick_backtests_run >= 0"),
        ("ck_nightly_pipeline_runs_full_bt_nonneg", "full_backtests_run >= 0"),
        ("ck_nightly_pipeline_runs_recs_nonneg", "recommendations_produced >= 0"),
    ]
    for name, expr in _pipeline_checks:
        if not _check_exists(conn, "nightly_pipeline_runs", name):
            op.create_check_constraint(name, "nightly_pipeline_runs", expr)

    # -- ScannerJob engine_version CHECK -----------------------------------
    if not _check_exists(conn, "scanner_jobs", "ck_scanner_jobs_valid_engine_version"):
        op.create_check_constraint(
            "ck_scanner_jobs_valid_engine_version",
            "scanner_jobs",
            "engine_version IN ('options-multileg-v1', 'options-multileg-v2')",
        )

    # -- SweepJob engine_version CHECK -------------------------------------
    if not _check_exists(conn, "sweep_jobs", "ck_sweep_jobs_valid_engine_version"):
        op.create_check_constraint(
            "ck_sweep_jobs_valid_engine_version",
            "sweep_jobs",
            "engine_version IN ('options-multileg-v1', 'options-multileg-v2')",
        )

    # -- StripeEvent idempotency_status server_default fix -----------------
    op.alter_column(
        "stripe_events",
        "idempotency_status",
        server_default="processing",
    )

    # -- daily_recommendations updated_at trigger --------------------------
    op.execute(
        "DROP TRIGGER IF EXISTS trg_daily_recommendations_updated_at ON daily_recommendations;"
    )
    op.execute(
        "CREATE TRIGGER trg_daily_recommendations_updated_at "
        "BEFORE UPDATE ON daily_recommendations "
        "FOR EACH ROW EXECUTE FUNCTION set_updated_at();"
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS trg_daily_recommendations_updated_at ON daily_recommendations;"
    )

    op.alter_column(
        "stripe_events",
        "idempotency_status",
        server_default="processed",
    )

    op.execute("ALTER TABLE sweep_jobs DROP CONSTRAINT IF EXISTS ck_sweep_jobs_valid_engine_version")
    op.execute("ALTER TABLE scanner_jobs DROP CONSTRAINT IF EXISTS ck_scanner_jobs_valid_engine_version")

    _pipeline_checks = [
        "ck_nightly_pipeline_runs_recs_nonneg",
        "ck_nightly_pipeline_runs_full_bt_nonneg",
        "ck_nightly_pipeline_runs_quick_bt_nonneg",
        "ck_nightly_pipeline_runs_pairs_nonneg",
        "ck_nightly_pipeline_runs_symbols_after_nonneg",
        "ck_nightly_pipeline_runs_symbols_screened_nonneg",
    ]
    for name in _pipeline_checks:
        op.execute(f"ALTER TABLE nightly_pipeline_runs DROP CONSTRAINT IF EXISTS {name}")

    op.drop_index("ix_nightly_pipeline_runs_queued", table_name="nightly_pipeline_runs", if_exists=True)
    op.drop_index("ix_nightly_pipeline_runs_status_celery_created", table_name="nightly_pipeline_runs", if_exists=True)
    op.drop_index("ix_nightly_pipeline_runs_celery_task_id", table_name="nightly_pipeline_runs", if_exists=True)
