"""Migration safety tests: verify all Alembic migrations are well-formed and linear."""

from __future__ import annotations

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory


@pytest.fixture(scope="module")
def alembic_scripts() -> ScriptDirectory:
    config = Config("alembic.ini")
    return ScriptDirectory.from_config(config)


def test_all_migrations_have_upgrade_and_downgrade(alembic_scripts: ScriptDirectory) -> None:
    """Every migration script must define both upgrade() and downgrade()."""
    for script in alembic_scripts.walk_revisions():
        module = script.module
        assert hasattr(module, "upgrade"), (
            f"Migration {script.revision} ({script.path}) is missing upgrade()"
        )
        assert callable(module.upgrade), (
            f"Migration {script.revision}: upgrade is not callable"
        )
        assert hasattr(module, "downgrade"), (
            f"Migration {script.revision} ({script.path}) is missing downgrade()"
        )
        assert callable(module.downgrade), (
            f"Migration {script.revision}: downgrade is not callable"
        )


def test_migration_chain_is_linear(alembic_scripts: ScriptDirectory) -> None:
    """The migration chain must have no branches (each revision has at most one child)."""
    children: dict[str | None, list[str]] = {}
    for script in alembic_scripts.walk_revisions():
        parent = script.down_revision
        if isinstance(parent, tuple):
            pytest.fail(
                f"Migration {script.revision} has multiple parents (merge migration). "
                f"The chain must be strictly linear. Parents: {parent}"
            )
        children.setdefault(parent, []).append(script.revision)

    for parent_rev, child_revs in children.items():
        assert len(child_revs) == 1, (
            f"Branching detected: revision {parent_rev!r} has multiple children: {child_revs}. "
            f"The migration chain must be linear."
        )


def test_latest_revision_matches_alembic_head(alembic_scripts: ScriptDirectory) -> None:
    """The head revision reported by ScriptDirectory must be a single head, not multiple."""
    heads = alembic_scripts.get_heads()
    assert len(heads) == 1, (
        f"Expected exactly 1 alembic head but found {len(heads)}: {heads}. "
        f"This indicates a branch in the migration chain."
    )
    all_revisions = [rev.revision for rev in alembic_scripts.walk_revisions()]
    assert heads[0] == all_revisions[0], (
        f"Head revision {heads[0]} does not match the first revision in walk order {all_revisions[0]}"
    )


def test_no_duplicate_revision_ids(alembic_scripts: ScriptDirectory) -> None:
    """Each revision ID must be unique across all migrations."""
    revisions = [rev.revision for rev in alembic_scripts.walk_revisions()]
    seen: dict[str, int] = {}
    for rev in revisions:
        seen[rev] = seen.get(rev, 0) + 1
    duplicates = {rev: count for rev, count in seen.items() if count > 1}
    assert not duplicates, f"Duplicate revision IDs found: {duplicates}"


def test_down_revision_chain_is_contiguous(alembic_scripts: ScriptDirectory) -> None:
    """Every down_revision must reference an existing revision (except the first migration)."""
    all_revisions = {rev.revision for rev in alembic_scripts.walk_revisions()}
    for script in alembic_scripts.walk_revisions():
        if script.down_revision is None:
            continue
        down = script.down_revision
        if isinstance(down, tuple):
            for d in down:
                assert d in all_revisions, (
                    f"Migration {script.revision} references non-existent down_revision {d}"
                )
        else:
            assert down in all_revisions, (
                f"Migration {script.revision} references non-existent down_revision {down}"
            )
