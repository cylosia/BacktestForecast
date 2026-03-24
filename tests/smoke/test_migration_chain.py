"""Verify the Alembic migration chain has no duplicate revision IDs."""
from __future__ import annotations

import pytest


@pytest.mark.smoke
def test_migration_chain_has_no_duplicate_revisions():
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    config = Config("alembic.ini")
    scripts = ScriptDirectory.from_config(config)
    revisions = [rev.revision for rev in scripts.walk_revisions()]
    assert len(revisions) == len(set(revisions)), (
        f"Duplicate revisions found: {[r for r in revisions if revisions.count(r) > 1]}"
    )
