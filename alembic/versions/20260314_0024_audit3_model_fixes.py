"""Audit 3 model fixes: indexes, column types, new columns.

Revision ID: 20260314_0024
Revises: 20260314_0023
Create Date: 2026-03-14
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260314_0024"
down_revision = "20260314_0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_symbol_analyses_celery_task_id",
        "symbol_analyses",
        ["celery_task_id"],
    )

    with op.batch_alter_table("symbol_analyses") as batch_op:
        batch_op.alter_column(
            "celery_task_id",
            existing_type=sa.String(255),
            type_=sa.String(64),
            existing_nullable=True,
        )

    with op.batch_alter_table("nightly_pipeline_runs") as batch_op:
        batch_op.add_column(sa.Column("started_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            )
        )

    with op.batch_alter_table("daily_recommendations") as batch_op:
        batch_op.alter_column(
            "strategy_type",
            existing_type=sa.String(64),
            type_=sa.String(32),
            existing_nullable=False,
        )
        batch_op.alter_column(
            "regime_labels",
            existing_type=sa.Text(),
            type_=sa.dialects.postgresql.JSONB(),
            existing_nullable=False,
            server_default=sa.text("'[]'::jsonb"),
            postgresql_using="to_jsonb(string_to_array(regime_labels, ','))",
        )

    op.create_index(
        "ix_backtest_runs_status_celery_created",
        "backtest_runs",
        ["status", "celery_task_id", "created_at"],
    )
    op.create_index(
        "ix_export_jobs_status_celery_created",
        "export_jobs",
        ["status", "celery_task_id", "created_at"],
    )
    op.create_index(
        "ix_scanner_jobs_status_celery_created",
        "scanner_jobs",
        ["status", "celery_task_id", "created_at"],
    )
    op.create_index(
        "ix_symbol_analyses_status_celery_created",
        "symbol_analyses",
        ["status", "celery_task_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_symbol_analyses_status_celery_created", table_name="symbol_analyses")
    op.drop_index("ix_scanner_jobs_status_celery_created", table_name="scanner_jobs")
    op.drop_index("ix_export_jobs_status_celery_created", table_name="export_jobs")
    op.drop_index("ix_backtest_runs_status_celery_created", table_name="backtest_runs")

    with op.batch_alter_table("daily_recommendations") as batch_op:
        batch_op.alter_column(
            "regime_labels",
            existing_type=sa.JSON(),
            type_=sa.Text(),
            existing_nullable=False,
        )
        batch_op.alter_column(
            "strategy_type",
            existing_type=sa.String(32),
            type_=sa.String(64),
            existing_nullable=False,
        )

    with op.batch_alter_table("nightly_pipeline_runs") as batch_op:
        batch_op.drop_column("updated_at")
        batch_op.drop_column("started_at")

    with op.batch_alter_table("symbol_analyses") as batch_op:
        batch_op.alter_column(
            "celery_task_id",
            existing_type=sa.String(64),
            type_=sa.String(255),
            existing_nullable=True,
        )

    op.drop_index("ix_symbol_analyses_celery_task_id", table_name="symbol_analyses")
