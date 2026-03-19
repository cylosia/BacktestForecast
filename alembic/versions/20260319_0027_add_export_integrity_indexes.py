"""Add export integrity indexes and scanner ranking_version constraint.

- ix_export_jobs_sha256_hex on export_jobs(sha256_hex)
- ix_export_jobs_storage_key on export_jobs(storage_key)
- ck_scanner_jobs_valid_ranking_version CHECK constraint

Revision ID: 20260319_0027
Revises: 20260319_0026
Create Date: 2026-03-19
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260319_0027"
down_revision = "20260319_0026"
branch_labels = None
depends_on = None


def _index_exists(name: str) -> bool:
    bind = op.get_bind()
    row = bind.execute(
        sa.text(
            "SELECT 1 FROM pg_indexes WHERE indexname = :name"
        ),
        {"name": name},
    ).fetchone()
    return row is not None


def _constraint_exists(table: str, name: str) -> bool:
    bind = op.get_bind()
    row = bind.execute(
        sa.text(
            "SELECT 1 FROM information_schema.table_constraints "
            "WHERE table_name = :tbl AND constraint_name = :name"
        ),
        {"tbl": table, "name": name},
    ).fetchone()
    return row is not None


def upgrade() -> None:
    if not _index_exists("ix_export_jobs_sha256_hex"):
        op.create_index("ix_export_jobs_sha256_hex", "export_jobs", ["sha256_hex"])

    if not _index_exists("ix_export_jobs_storage_key"):
        op.create_index("ix_export_jobs_storage_key", "export_jobs", ["storage_key"])

    if not _constraint_exists("scanner_jobs", "ck_scanner_jobs_valid_ranking_version"):
        op.execute(sa.text(
            "ALTER TABLE scanner_jobs "
            "ADD CONSTRAINT ck_scanner_jobs_valid_ranking_version "
            "CHECK (ranking_version IN ('scanner-ranking-v1', 'scanner-ranking-v2')) "
            "NOT VALID"
        ))
        op.execute(sa.text(
            "ALTER TABLE scanner_jobs VALIDATE CONSTRAINT ck_scanner_jobs_valid_ranking_version"
        ))


def downgrade() -> None:
    if _constraint_exists("scanner_jobs", "ck_scanner_jobs_valid_ranking_version"):
        op.drop_constraint("ck_scanner_jobs_valid_ranking_version", "scanner_jobs", type_="check")

    if _index_exists("ix_export_jobs_storage_key"):
        op.drop_index("ix_export_jobs_storage_key", "export_jobs")

    if _index_exists("ix_export_jobs_sha256_hex"):
        op.drop_index("ix_export_jobs_sha256_hex", "export_jobs")
