"""Add CHECK constraint preventing empty template names.

Revision ID: 20260318_0026
Revises: 20260318_0025
Create Date: 2026-03-18
"""
import sqlalchemy as sa
from alembic import op

revision = "20260318_0026"
down_revision = "20260318_0025"
branch_labels = None
depends_on = None

_CONSTRAINT_NAME = "ck_backtest_templates_name_not_empty"


def upgrade() -> None:
    conn = op.get_bind()
    result = conn.execute(sa.text(
        "SELECT 1 FROM information_schema.table_constraints "
        "WHERE constraint_name = :name AND table_name = 'backtest_templates'"
    ), {"name": _CONSTRAINT_NAME})
    if result.fetchone() is None:
        op.execute(sa.text(
            f"ALTER TABLE backtest_templates "
            f"ADD CONSTRAINT {_CONSTRAINT_NAME} CHECK (length(name) > 0) NOT VALID"
        ))
        op.execute(sa.text(
            f"ALTER TABLE backtest_templates VALIDATE CONSTRAINT {_CONSTRAINT_NAME}"
        ))


def downgrade() -> None:
    op.execute(sa.text(
        f"ALTER TABLE backtest_templates DROP CONSTRAINT IF EXISTS {_CONSTRAINT_NAME}"
    ))
