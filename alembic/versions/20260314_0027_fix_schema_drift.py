"""Fix schema drift: add missing symbol_analyses user_id index.

Revision ID: 20260314_0027
Revises: 20260314_0026
Create Date: 2026-03-14
"""
from __future__ import annotations

from alembic import op

revision = "20260314_0027"
down_revision = "20260314_0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_symbol_analyses_user_id", "symbol_analyses", ["user_id"])


def downgrade() -> None:
    op.drop_index("ix_symbol_analyses_user_id", table_name="symbol_analyses")
