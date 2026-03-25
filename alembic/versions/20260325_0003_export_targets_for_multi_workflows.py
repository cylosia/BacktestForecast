"""Support export jobs for multi-symbol and multi-step workflows.

Revision ID: 20260325_0003
Revises: 20260325_0002
Create Date: 2026-03-25 16:50:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260325_0003"
down_revision = "20260325_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("export_jobs", sa.Column("multi_symbol_run_id", sa.Uuid(), nullable=True))
    op.add_column("export_jobs", sa.Column("multi_step_run_id", sa.Uuid(), nullable=True))
    op.add_column("export_jobs", sa.Column("export_target_kind", sa.String(length=24), nullable=False, server_default="backtest"))

    op.alter_column("export_jobs", "backtest_run_id", existing_type=sa.Uuid(), nullable=True)

    op.create_index("ix_export_jobs_multi_symbol_run_id", "export_jobs", ["multi_symbol_run_id"], unique=False)
    op.create_index("ix_export_jobs_multi_step_run_id", "export_jobs", ["multi_step_run_id"], unique=False)

    op.create_foreign_key(
        "fk_export_jobs_multi_symbol_run_id",
        "export_jobs",
        "multi_symbol_runs",
        ["multi_symbol_run_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_export_jobs_multi_step_run_id",
        "export_jobs",
        "multi_step_runs",
        ["multi_step_run_id"],
        ["id"],
        ondelete="CASCADE",
    )

    op.create_check_constraint(
        "ck_export_jobs_valid_target_kind",
        "export_jobs",
        "export_target_kind IN ('backtest', 'multi_symbol', 'multi_step')",
    )
    op.create_check_constraint(
        "ck_export_jobs_exactly_one_target",
        "export_jobs",
        "((CASE WHEN backtest_run_id IS NOT NULL THEN 1 ELSE 0 END) + "
        "(CASE WHEN multi_symbol_run_id IS NOT NULL THEN 1 ELSE 0 END) + "
        "(CASE WHEN multi_step_run_id IS NOT NULL THEN 1 ELSE 0 END)) = 1",
    )

    op.execute("UPDATE export_jobs SET export_target_kind = 'backtest' WHERE export_target_kind IS NULL")


def downgrade() -> None:
    op.drop_constraint("ck_export_jobs_exactly_one_target", "export_jobs", type_="check")
    op.drop_constraint("ck_export_jobs_valid_target_kind", "export_jobs", type_="check")
    op.drop_constraint("fk_export_jobs_multi_step_run_id", "export_jobs", type_="foreignkey")
    op.drop_constraint("fk_export_jobs_multi_symbol_run_id", "export_jobs", type_="foreignkey")
    op.drop_index("ix_export_jobs_multi_step_run_id", table_name="export_jobs")
    op.drop_index("ix_export_jobs_multi_symbol_run_id", table_name="export_jobs")
    op.alter_column("export_jobs", "backtest_run_id", existing_type=sa.Uuid(), nullable=False)
    op.drop_column("export_jobs", "export_target_kind")
    op.drop_column("export_jobs", "multi_step_run_id")
    op.drop_column("export_jobs", "multi_symbol_run_id")
