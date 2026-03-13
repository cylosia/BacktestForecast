"""Add constraints, defaults, and indexes reconciliation.

Revision ID: 20260313_0017
Revises: 20260313_0016
Create Date: 2026-03-13

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260313_0017"
down_revision = "20260313_0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Partial unique index: one succeeded run per trade_date
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_pipeline_runs_succeeded_trade_date
        ON nightly_pipeline_runs (trade_date)
        WHERE status = 'succeeded'
        """
    )

    # 2. DailyRecommendation(pipeline_run_id, rank) uniqueness - add idempotently
    op.execute(
        """
        DO $$
        BEGIN
            ALTER TABLE daily_recommendations
            ADD CONSTRAINT uq_daily_recs_pipeline_rank
            UNIQUE (pipeline_run_id, rank);
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )

    # 3. Reconcile missing ORM CheckConstraints
    op.execute(
        """
        DO $$
        BEGIN
            ALTER TABLE users ADD CONSTRAINT valid_plan_tier
            CHECK (plan_tier IN ('free', 'pro', 'premium'));
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            ALTER TABLE backtest_runs ADD CONSTRAINT valid_run_status
            CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled'));
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            ALTER TABLE scanner_jobs ADD CONSTRAINT valid_job_status
            CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled'));
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            ALTER TABLE export_jobs ADD CONSTRAINT valid_export_status
            CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled'));
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            ALTER TABLE nightly_pipeline_runs ADD CONSTRAINT valid_pipeline_status
            CHECK (status IN ('running', 'succeeded', 'failed'));
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            ALTER TABLE symbol_analyses ADD CONSTRAINT valid_analysis_status
            CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled'));
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )

    # 4. Reconcile server-default drift
    op.alter_column("scanner_jobs", "status", server_default="queued")
    op.alter_column("scanner_jobs", "refresh_daily", server_default=sa.text("false"))
    op.alter_column("scanner_jobs", "refresh_priority", server_default="0")
    op.alter_column("scanner_jobs", "candidate_count", server_default="0")
    op.alter_column("scanner_jobs", "evaluated_candidate_count", server_default="0")
    op.alter_column("scanner_jobs", "recommendation_count", server_default="0")
    op.alter_column("export_jobs", "status", server_default="queued")
    op.alter_column("export_jobs", "size_bytes", server_default="0")
    op.alter_column("symbol_analyses", "status", server_default="queued")
    op.alter_column("nightly_pipeline_runs", "status", server_default="running")

    # 5. Compound index for stale analysis reaping
    op.create_index(
        "ix_symbol_analyses_status_created",
        "symbol_analyses",
        ["status", "created_at"],
    )


def downgrade() -> None:
    # 5. Drop compound index
    op.drop_index("ix_symbol_analyses_status_created", table_name="symbol_analyses")

    # 4. Remove server defaults (only export_jobs.size_bytes was explicitly removed in 0004;
    #    status defaults come from 0016; scanner_jobs defaults from 0003)
    op.alter_column("export_jobs", "size_bytes", server_default=None)

    # 3. Drop check constraints
    op.execute("ALTER TABLE symbol_analyses DROP CONSTRAINT IF EXISTS valid_analysis_status")
    op.execute("ALTER TABLE nightly_pipeline_runs DROP CONSTRAINT IF EXISTS valid_pipeline_status")
    op.execute("ALTER TABLE export_jobs DROP CONSTRAINT IF EXISTS valid_export_status")
    op.execute("ALTER TABLE scanner_jobs DROP CONSTRAINT IF EXISTS valid_job_status")
    op.execute("ALTER TABLE backtest_runs DROP CONSTRAINT IF EXISTS valid_run_status")
    op.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS valid_plan_tier")

    # 2. Drop unique constraint on daily_recommendations
    op.execute("ALTER TABLE daily_recommendations DROP CONSTRAINT IF EXISTS uq_daily_recs_pipeline_rank")

    # 1. Drop partial unique index
    op.execute("DROP INDEX IF EXISTS uq_pipeline_runs_succeeded_trade_date")
