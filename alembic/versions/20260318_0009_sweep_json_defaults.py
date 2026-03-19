"""Add server_default for non-nullable sweep JSON columns.

Revision ID: 20260318_0009
Revises: 20260318_0008
Create Date: 2026-03-18
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260318_0009"
down_revision = "20260318_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("sweep_jobs", "request_snapshot_json", server_default=sa.text("'{}'::jsonb"))
    op.alter_column("sweep_results", "parameter_snapshot_json", server_default=sa.text("'{}'::jsonb"))
    op.alter_column("sweep_results", "summary_json", server_default=sa.text("'{}'::jsonb"))
    op.alter_column("sweep_results", "trades_json", server_default=sa.text("'[]'::jsonb"))
    op.alter_column("sweep_results", "equity_curve_json", server_default=sa.text("'[]'::jsonb"))


def downgrade() -> None:
    op.alter_column("sweep_results", "equity_curve_json", server_default=None)
    op.alter_column("sweep_results", "trades_json", server_default=None)
    op.alter_column("sweep_results", "summary_json", server_default=None)
    op.alter_column("sweep_results", "parameter_snapshot_json", server_default=None)
    op.alter_column("sweep_jobs", "request_snapshot_json", server_default=None)
