"""Backfill risk_free_rate from input_snapshot_json for existing runs.

Migration 0031 added the risk_free_rate column as nullable but did not
backfill it.  The service layer falls back to input_snapshot_json, but
having the column populated enables direct SQL queries and avoids the
runtime fallback chain.

This migration reads risk_free_rate from input_snapshot_json for each
row where the column is still NULL and updates it in batches.

Revision ID: 20260319_0040
Revises: 20260319_0039
"""
from alembic import op
import sqlalchemy as sa


revision = "20260319_0040"
down_revision = "20260319_0039"
branch_labels = None
depends_on = None

_BATCH_SIZE = 500


def upgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.dialect.name == "postgresql"
    if not is_postgres:
        return

    total = 0
    while True:
        result = bind.execute(
            sa.text(
                "UPDATE backtest_runs "
                "SET risk_free_rate = (input_snapshot_json->>'risk_free_rate')::numeric(6,4) "
                "WHERE id IN ("
                "  SELECT id FROM backtest_runs "
                "  WHERE risk_free_rate IS NULL "
                "  AND input_snapshot_json->>'risk_free_rate' IS NOT NULL "
                "  LIMIT :batch"
                ")"
            ),
            {"batch": _BATCH_SIZE},
        )
        updated = result.rowcount
        total += updated
        if updated < _BATCH_SIZE:
            break

    if total > 0:
        op.execute(sa.text("SELECT 1"))


def downgrade() -> None:
    pass
