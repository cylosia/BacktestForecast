"""Add operational indexes for async workflows and sweep dedupe.

Revision ID: 20260328_0009
Revises: 20260327_0008
Create Date: 2026-03-28 10:45:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.exc import NoSuchTableError

from alembic import op

revision = "20260328_0009"
down_revision = "20260327_0008"
branch_labels = None
depends_on = None


_INDEX_SPECS: dict[str, tuple[tuple[str, list[str], dict[str, object]], ...]] = {
    "multi_symbol_runs": (
        ("ix_multi_symbol_runs_celery_task_id", ["celery_task_id"], {}),
        ("ix_multi_symbol_runs_status_celery_created", ["status", "celery_task_id", "created_at"], {}),
        ("ix_multi_symbol_runs_dispatch_started_at", ["dispatch_started_at"], {}),
        ("ix_multi_symbol_runs_queued", ["created_at"], {"postgresql_where": sa.text("status = 'queued'")}),
    ),
    "multi_step_runs": (
        ("ix_multi_step_runs_status", ["status"], {}),
        ("ix_multi_step_runs_celery_task_id", ["celery_task_id"], {}),
        ("ix_multi_step_runs_status_celery_created", ["status", "celery_task_id", "created_at"], {}),
        ("ix_multi_step_runs_dispatch_started_at", ["dispatch_started_at"], {}),
        ("ix_multi_step_runs_queued", ["created_at"], {"postgresql_where": sa.text("status = 'queued'")}),
    ),
    "multi_symbol_trade_groups": (
        ("ix_multi_symbol_trade_groups_run_entry_date", ["run_id", "entry_date"], {}),
    ),
    "multi_symbol_trades": (
        ("ix_multi_symbol_trades_run_entry_date", ["run_id", "entry_date"], {}),
    ),
    "multi_step_step_events": (
        ("ix_multi_step_step_events_run_event_at", ["run_id", "event_at"], {}),
    ),
    "multi_step_trades": (
        ("ix_multi_step_trades_run_entry_date", ["run_id", "entry_date"], {}),
    ),
    "sweep_jobs": (
        (
            "ix_sweep_jobs_active_dedup_lookup",
            ["user_id", "symbol", "request_hash", "created_at"],
            {"postgresql_where": sa.text("status IN ('queued', 'running') AND request_hash IS NOT NULL")},
        ),
    ),
}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    for table_name, specs in _INDEX_SPECS.items():
        try:
            existing_indexes = {index["name"] for index in inspector.get_indexes(table_name)}
        except NoSuchTableError:
            continue
        for index_name, columns, kwargs in specs:
            if index_name in existing_indexes:
                continue
            op.create_index(index_name, table_name, columns, unique=False, **kwargs)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    for table_name, specs in _INDEX_SPECS.items():
        try:
            existing_indexes = {index["name"] for index in inspector.get_indexes(table_name)}
        except NoSuchTableError:
            continue
        for index_name, _columns, _kwargs in reversed(specs):
            if index_name in existing_indexes:
                op.drop_index(index_name, table_name=table_name)
