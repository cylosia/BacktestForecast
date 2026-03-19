"""Schema drift fixes: daily_recommendations trigger + merge orphan branch.

1. Add updated_at trigger to daily_recommendations (was missing from the
   original trigger migration 20260318_0014).
2. Merge the orphaned 0024_heartbeat branch into the main chain. That
   migration's effects (heartbeat columns, trade index) already exist
   from the baseline and later migrations, so no DDL is needed — this
   simply closes the dangling head.

Revision ID: 20260319_0034
Revises: 20260319_0033, 0024_heartbeat
Create Date: 2026-03-19
"""
from alembic import op

revision = "20260319_0034"
down_revision = ("20260319_0033", "0024_heartbeat")
branch_labels = None
depends_on = None

_FUNCTION_EXISTS_CHECK = """
DO $$ BEGIN
    CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS TRIGGER AS $fn$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $fn$ LANGUAGE plpgsql;
END $$;
"""


def upgrade() -> None:
    op.execute(_FUNCTION_EXISTS_CHECK)

    op.execute("DROP TRIGGER IF EXISTS trg_daily_recommendations_updated_at ON daily_recommendations;")
    op.execute(
        "CREATE TRIGGER trg_daily_recommendations_updated_at "
        "BEFORE UPDATE ON daily_recommendations "
        "FOR EACH ROW EXECUTE FUNCTION set_updated_at();"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_daily_recommendations_updated_at ON daily_recommendations;")
