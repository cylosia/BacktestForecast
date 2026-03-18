"""Convert sweep table JSON columns to JSONB and add missing server_default.

Revision ID: 20260318_0021
Revises: 20260318_0020
Create Date: 2026-03-18

Fixes migration 0006 which used sa.JSON instead of JSONB, and adds missing
server_default values on NOT NULL JSON columns.
"""
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20260318_0021"
down_revision = "20260318_0020"
branch_labels = None
depends_on = None

_SWEEP_JOBS_JSON_COLS = [
    ("request_snapshot_json", False, "'{}'::jsonb"),
    ("warnings_json", False, "'[]'::jsonb"),
    ("prefetch_summary_json", True, None),
]

_SWEEP_RESULTS_JSON_COLS = [
    ("parameter_snapshot_json", False, "'{}'::jsonb"),
    ("summary_json", False, "'{}'::jsonb"),
    ("warnings_json", False, "'[]'::jsonb"),
    ("trades_json", False, "'[]'::jsonb"),
    ("equity_curve_json", False, "'[]'::jsonb"),
]


def upgrade() -> None:
    for col_name, nullable, server_default in _SWEEP_JOBS_JSON_COLS:
        op.execute(
            f"ALTER TABLE sweep_jobs "
            f"ALTER COLUMN {col_name} SET DATA TYPE jsonb USING {col_name}::jsonb"
        )
        if server_default is not None:
            op.execute(
                f"ALTER TABLE sweep_jobs "
                f"ALTER COLUMN {col_name} SET DEFAULT {server_default}"
            )

    for col_name, nullable, server_default in _SWEEP_RESULTS_JSON_COLS:
        op.execute(
            f"ALTER TABLE sweep_results "
            f"ALTER COLUMN {col_name} SET DATA TYPE jsonb USING {col_name}::jsonb"
        )
        if server_default is not None:
            op.execute(
                f"ALTER TABLE sweep_results "
                f"ALTER COLUMN {col_name} SET DEFAULT {server_default}"
            )


def downgrade() -> None:
    for col_name, _, _ in _SWEEP_JOBS_JSON_COLS:
        op.execute(
            f"ALTER TABLE sweep_jobs "
            f"ALTER COLUMN {col_name} SET DATA TYPE json USING {col_name}::json"
        )
        op.execute(
            f"ALTER TABLE sweep_jobs ALTER COLUMN {col_name} DROP DEFAULT"
        )

    for col_name, _, _ in _SWEEP_RESULTS_JSON_COLS:
        op.execute(
            f"ALTER TABLE sweep_results "
            f"ALTER COLUMN {col_name} SET DATA TYPE json USING {col_name}::json"
        )
        op.execute(
            f"ALTER TABLE sweep_results ALTER COLUMN {col_name} DROP DEFAULT"
        )
