"""Add explicit FK relationship between NightlyPipelineRun and ScannerJob.

Adds pipeline_run_id column to scanner_jobs for explicit linkage instead of
implicit trade_date matching. Also adds index for efficient lookups.

Revision ID: 0035
Revises: 0034
"""
import uuid

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.engine import Dialect
from sqlalchemy.types import CHAR, TypeDecorator, TypeEngine


class GUID(TypeDecorator):
    """Frozen copy — do not import from application code in migrations."""
    impl = CHAR(36)
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect: Dialect):
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value if dialect.name == "postgresql" else str(value)
        coerced = uuid.UUID(str(value))
        return coerced if dialect.name == "postgresql" else str(coerced)

    def process_result_value(self, value, dialect: Dialect):
        if value is None or isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))


revision = "20260319_0035"
down_revision = "20260319_0034"
branch_labels = None
depends_on = None


def _column_exists(table: str, column: str) -> bool:
    bind = op.get_bind()
    row = bind.execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :tbl AND column_name = :col"
        ),
        {"tbl": table, "col": column},
    ).fetchone()
    return row is not None


def _index_exists(name: str) -> bool:
    bind = op.get_bind()
    result = bind.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE indexname = :name"),
        {"name": name},
    ).fetchone()
    return result is not None


def _constraint_exists(name: str) -> bool:
    bind = op.get_bind()
    result = bind.execute(
        sa.text("SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = :name"),
        {"name": name},
    ).fetchone()
    return result is not None


def upgrade() -> None:
    if not _column_exists("scanner_jobs", "pipeline_run_id"):
        op.add_column(
            "scanner_jobs",
            sa.Column("pipeline_run_id", GUID(), nullable=True),
        )
    if not _constraint_exists("fk_scanner_jobs_pipeline_run_id"):
        op.create_foreign_key(
            "fk_scanner_jobs_pipeline_run_id",
            "scanner_jobs",
            "nightly_pipeline_runs",
            ["pipeline_run_id"],
            ["id"],
            ondelete="SET NULL",
        )
    if not _index_exists("ix_scanner_jobs_pipeline_run_id"):
        op.create_index(
            "ix_scanner_jobs_pipeline_run_id",
            "scanner_jobs",
            ["pipeline_run_id"],
        )


def downgrade() -> None:
    if _index_exists("ix_scanner_jobs_pipeline_run_id"):
        op.drop_index("ix_scanner_jobs_pipeline_run_id", table_name="scanner_jobs")
    if _constraint_exists("fk_scanner_jobs_pipeline_run_id"):
        op.drop_constraint("fk_scanner_jobs_pipeline_run_id", "scanner_jobs", type_="foreignkey")
    if _column_exists("scanner_jobs", "pipeline_run_id"):
        op.drop_column("scanner_jobs", "pipeline_run_id")
