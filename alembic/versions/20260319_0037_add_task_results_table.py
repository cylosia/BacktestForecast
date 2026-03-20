"""Add task_results table for structured task outcome tracking.

Replaces Redis-based result storage with a durable DB table that
supports querying historical task performance, SLA tracking, and
post-mortem analysis.

Revision ID: 20260319_0037
Revises: 20260319_0036
"""
import uuid

from alembic import op
import sqlalchemy as sa
from sqlalchemy import JSON
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.engine import Dialect
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement
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


class _JsonDefault(ClauseElement):
    __visit_name__ = "json_default"
    def __init__(self, value: str) -> None:
        self.value = value

@compiles(_JsonDefault, "postgresql")
def _pg(element, compiler, **kw):
    return f"'{element.value}'::jsonb"

@compiles(_JsonDefault, "sqlite")
def _sqlite(element, compiler, **kw):
    return f"'{element.value}'"


JSON_VARIANT = JSON().with_variant(JSONB, "postgresql")
JSON_DEFAULT_EMPTY_OBJECT = _JsonDefault("{}")


revision = "20260319_0037"
down_revision = "20260319_0036"
branch_labels = None
depends_on = None


def _table_exists(name: str) -> bool:
    bind = op.get_bind()
    result = bind.execute(
        sa.text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = :name AND table_schema = 'public'"
        ),
        {"name": name},
    ).fetchone()
    return result is not None


def upgrade() -> None:
    if _table_exists("task_results"):
        return
    op.create_table(
        "task_results",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("task_name", sa.String(128), nullable=False),
        sa.Column("task_id", sa.String(64), nullable=False, unique=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("correlation_id", GUID(), nullable=True),
        sa.Column("correlation_type", sa.String(64), nullable=True),
        sa.Column("duration_seconds", sa.Numeric(10, 3), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("result_summary_json", JSON_VARIANT, nullable=False, server_default=JSON_DEFAULT_EMPTY_OBJECT),
        sa.Column("worker_hostname", sa.String(255), nullable=True),
        sa.Column("retries", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_task_results_task_name_created", "task_results", ["task_name", "created_at"])
    op.create_index("ix_task_results_correlation_id", "task_results", ["correlation_id"])
    op.create_index("ix_task_results_status_created", "task_results", ["status", "created_at"])
    op.create_check_constraint(
        "ck_task_results_valid_status",
        "task_results",
        "status IN ('succeeded', 'failed', 'retried', 'timeout')",
    )


def downgrade() -> None:
    if _table_exists("task_results"):
        op.drop_table("task_results")
