"""Add CHECK constraint preventing empty-string scanner job names.

ScannerJob.name is nullable (NULL = unnamed), but empty string "" should
not be allowed as it is semantically different from NULL and bypasses
UI display logic. This matches the pattern used by BacktestTemplate.name
(ck_backtest_templates_name_not_empty).

Existing rows with empty-string names are set to NULL before adding the
constraint.

Revision ID: 20260320_0046
Revises: 20260319_0044
Create Date: 2026-03-20
"""
from alembic import op
import sqlalchemy as sa


revision = "20260320_0046"
down_revision = "20260319_0044"
branch_labels = None
depends_on = None

_CONSTRAINT_NAME = "ck_scanner_jobs_name_not_empty"


def _constraint_exists(bind, table: str, constraint: str) -> bool:
    result = bind.execute(sa.text(
        "SELECT 1 FROM information_schema.table_constraints "
        "WHERE table_name = :table AND constraint_name = :constraint"
    ), {"table": table, "constraint": constraint})
    return result.scalar() is not None


def upgrade() -> None:
    bind = op.get_bind()

    if _constraint_exists(bind, "scanner_jobs", _CONSTRAINT_NAME):
        return

    updated = bind.execute(sa.text(
        "UPDATE scanner_jobs SET name = NULL WHERE name = ''"
    )).rowcount
    if updated:
        import sys
        print(
            f"[migration 0046] Set {updated} empty scanner_jobs.name to NULL",
            file=sys.stderr,
        )

    op.create_check_constraint(
        _CONSTRAINT_NAME,
        "scanner_jobs",
        "name IS NULL OR length(name) > 0",
    )


def downgrade() -> None:
    op.drop_constraint(_CONSTRAINT_NAME, "scanner_jobs", type_="check")
