"""Drop redundant indexes and add missing CHECK constraints.

Fixes:
- Drop ix_scanner_recommendations_job_rank (redundant with uq_scanner_recommendations_job_rank)
- Drop ix_backtest_equity_points_run_date (redundant with uq_backtest_equity_points_run_date)
- Add CHECK constraints on scanner_jobs.mode and scanner_jobs.job_kind
- Add missing composite indexes declared in models

NOTE: For production, consider running CREATE INDEX CONCURRENTLY manually
to avoid blocking writes.

Revision ID: 20260314_0032
Revises: 20260314_0031
Create Date: 2026-03-14
"""
from __future__ import annotations

from alembic import op

revision = "20260314_0032"
down_revision = "20260314_0031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        op.f("ix_scanner_recommendations_job_rank"),
        table_name="scanner_recommendations",
        if_exists=True,
    )
    op.drop_index(
        op.f("ix_backtest_equity_points_run_date"),
        table_name="backtest_equity_points",
        if_exists=True,
    )

    op.create_check_constraint(
        op.f("ck_scanner_jobs_valid_mode"),
        "scanner_jobs",
        "mode IN ('basic', 'advanced', 'pro')",
    )
    op.create_check_constraint(
        op.f("ck_scanner_jobs_valid_job_kind"),
        "scanner_jobs",
        "job_kind IN ('manual', 'refresh', 'nightly')",
    )

    for idx_name, tbl, cols in [
        ("ix_backtest_runs_status_celery_created", "backtest_runs", ["status", "celery_task_id", "created_at"]),
        ("ix_export_jobs_status_celery_created", "export_jobs", ["status", "celery_task_id", "created_at"]),
        ("ix_export_jobs_status_expires_at", "export_jobs", ["status", "expires_at"]),
        ("ix_scanner_jobs_status_celery_created", "scanner_jobs", ["status", "celery_task_id", "created_at"]),
        ("ix_symbol_analyses_status_celery_created", "symbol_analyses", ["status", "celery_task_id", "created_at"]),
    ]:
        op.drop_index(op.f(idx_name), table_name=tbl, if_exists=True)
        op.create_index(op.f(idx_name), tbl, cols)

    op.drop_index("ix_nightly_pipeline_runs_cursor", table_name="nightly_pipeline_runs", if_exists=True)
    op.create_index(
        op.f("ix_nightly_pipeline_runs_cursor"),
        "nightly_pipeline_runs",
        ["created_at", "id"],
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_nightly_pipeline_runs_cursor"), table_name="nightly_pipeline_runs")
    op.drop_index(op.f("ix_symbol_analyses_status_celery_created"), table_name="symbol_analyses")
    op.drop_index(op.f("ix_scanner_jobs_status_celery_created"), table_name="scanner_jobs")
    op.drop_index(op.f("ix_export_jobs_status_expires_at"), table_name="export_jobs")
    op.drop_index(op.f("ix_export_jobs_status_celery_created"), table_name="export_jobs")
    op.drop_index(op.f("ix_backtest_runs_status_celery_created"), table_name="backtest_runs")

    op.drop_constraint(op.f("ck_scanner_jobs_valid_job_kind"), "scanner_jobs", type_="check")
    op.drop_constraint(op.f("ck_scanner_jobs_valid_mode"), "scanner_jobs", type_="check")

    op.create_index(
        op.f("ix_backtest_equity_points_run_date"),
        "backtest_equity_points",
        ["run_id", "trade_date"],
    )
    op.create_index(
        op.f("ix_scanner_recommendations_job_rank"),
        "scanner_recommendations",
        ["scanner_job_id", "rank"],
    )
