"""Fix sweep JSON columns from json to jsonb, add updated_at triggers,
and set server_default on scanner_jobs.job_kind.

Revision ID: 20260318_0007
Revises: 20260317_0006
Create Date: 2026-03-18
"""
from __future__ import annotations

from alembic import op

revision = "20260318_0007"
down_revision = "20260317_0006"
branch_labels = None
depends_on = None

_SWEEP_JOBS_JSON_COLS = [
    "request_snapshot_json",
    "warnings_json",
    "prefetch_summary_json",
]

_SWEEP_RESULTS_JSON_COLS = [
    "parameter_snapshot_json",
    "summary_json",
    "warnings_json",
    "trades_json",
    "equity_curve_json",
]

_TRIGGER_FN = """\
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""


def upgrade() -> None:
    for col in _SWEEP_JOBS_JSON_COLS:
        op.execute(
            f"ALTER TABLE sweep_jobs "
            f"ALTER COLUMN {col} TYPE jsonb USING {col}::jsonb"
        )

    for col in _SWEEP_RESULTS_JSON_COLS:
        op.execute(
            f"ALTER TABLE sweep_results "
            f"ALTER COLUMN {col} TYPE jsonb USING {col}::jsonb"
        )

    op.execute(_TRIGGER_FN)

    op.execute("DROP TRIGGER IF EXISTS trg_sweep_jobs_updated_at ON sweep_jobs")
    op.execute(
        "CREATE TRIGGER trg_sweep_jobs_updated_at "
        "BEFORE UPDATE ON sweep_jobs "
        "FOR EACH ROW EXECUTE FUNCTION set_updated_at()"
    )
    op.execute("DROP TRIGGER IF EXISTS trg_sweep_results_updated_at ON sweep_results")
    op.execute(
        "CREATE TRIGGER trg_sweep_results_updated_at "
        "BEFORE UPDATE ON sweep_results "
        "FOR EACH ROW EXECUTE FUNCTION set_updated_at()"
    )

    op.alter_column("scanner_jobs", "job_kind", server_default="manual")


def downgrade() -> None:
    op.alter_column("scanner_jobs", "job_kind", server_default=None)

    op.execute("DROP TRIGGER IF EXISTS trg_sweep_results_updated_at ON sweep_results")
    op.execute("DROP TRIGGER IF EXISTS trg_sweep_jobs_updated_at ON sweep_jobs")
    op.execute("DROP FUNCTION IF EXISTS set_updated_at()")

    for col in _SWEEP_RESULTS_JSON_COLS:
        op.execute(
            f"ALTER TABLE sweep_results "
            f"ALTER COLUMN {col} TYPE json USING {col}::text::json"
        )

    for col in _SWEEP_JOBS_JSON_COLS:
        op.execute(
            f"ALTER TABLE sweep_jobs "
            f"ALTER COLUMN {col} TYPE json USING {col}::text::json"
        )
