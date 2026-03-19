"""Validate the NOT VALID sweep_jobs mode constraint.

Revision ID: 20260318_0024
Revises: 20260318_0023
Create Date: 2026-03-18

The constraint was added as NOT VALID in 20260318_0022. This migration
scans existing rows to verify they satisfy the CHECK, then marks the
constraint as VALID so it participates in query planning.
"""
import sqlalchemy as sa
from alembic import op

revision = "20260318_0024"
down_revision = "20260318_0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text(
        "ALTER TABLE sweep_jobs VALIDATE CONSTRAINT ck_sweep_jobs_valid_mode"
    ))


def downgrade() -> None:
    pass
