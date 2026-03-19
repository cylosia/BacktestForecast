"""Add GIN indexes on frequently-queried JSONB columns.

Revision ID: 20260318_0027
Revises: 20260318_0026
Create Date: 2026-03-18
"""
import sqlalchemy as sa
from alembic import op

revision = "20260318_0027"
down_revision = "20260319_0029"
branch_labels = None
depends_on = None


def _index_exists(name: str) -> bool:
    bind = op.get_bind()
    result = bind.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE indexname = :name"),
        {"name": name},
    )
    return result.fetchone() is not None


_INDEXES: list[tuple[str, str, str]] = [
    ("ix_scanner_recommendations_summary_gin", "scanner_recommendations", "summary_json"),
    ("ix_sweep_results_summary_gin", "sweep_results", "summary_json"),
]


def upgrade() -> None:
    for idx_name, table, column in _INDEXES:
        if not _index_exists(idx_name):
            op.execute(sa.text(
                f"CREATE INDEX IF NOT EXISTS {idx_name} "
                f"ON {table} USING gin ({column} jsonb_path_ops)"
            ))


def downgrade() -> None:
    for idx_name, _, _ in reversed(_INDEXES):
        if _index_exists(idx_name):
            op.execute(sa.text(f"DROP INDEX IF EXISTS {idx_name}"))
