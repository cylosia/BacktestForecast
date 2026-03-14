"""Add performance indexes for cleanup, quota, and reaper queries.

Revision ID: 20260314_0026
Revises: 20260314_0025
Create Date: 2026-03-14
"""
from __future__ import annotations

from alembic import op

revision = "20260314_0026"
down_revision = "20260314_0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ix_export_jobs_expires_at intentionally dropped in migration 0021 — do not re-create
    # ix_backtest_runs_user_status already created in migration 0001 — skip
    op.create_index("ix_backtest_runs_started_at", "backtest_runs", ["started_at"])


def downgrade() -> None:
    op.drop_index("ix_backtest_runs_started_at", table_name="backtest_runs")
