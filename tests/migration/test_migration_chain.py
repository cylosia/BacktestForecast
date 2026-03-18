"""Verify the Alembic migration chain is linear with no conflicts."""
from __future__ import annotations

import pathlib


def test_migration_chain_is_linear():
    """Each revision must have exactly one parent and no two files share a revision."""
    versions_dir = pathlib.Path(__file__).resolve().parents[2] / "alembic" / "versions"
    revisions: dict[str, str] = {}  # revision -> filename
    down_revisions: dict[str, list[str]] = {}  # down_revision -> [revision, ...]

    for py_file in sorted(versions_dir.glob("*.py")):
        if py_file.name == "__init__.py":
            continue
        text = py_file.read_text()
        rev = None
        down_rev = None
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("revision = "):
                rev = stripped.split("=", 1)[1].strip().strip('"').strip("'")
            if stripped.startswith("down_revision = "):
                val = stripped.split("=", 1)[1].strip().strip('"').strip("'")
                down_rev = val if val != "None" else None
        if rev is None:
            continue
        assert rev not in revisions, (
            f"Duplicate revision {rev!r} in {py_file.name} and {revisions[rev]}"
        )
        revisions[rev] = py_file.name
        key = down_rev or "__root__"
        down_revisions.setdefault(key, []).append(rev)

    # Check for multiple heads (branches)
    all_revs = set(revisions.keys())
    referenced_as_parent = {dr for dr in down_revisions if dr != "__root__"}
    heads = all_revs - referenced_as_parent
    children_of_each = {k: v for k, v in down_revisions.items() if len(v) > 1}
    for parent, children in children_of_each.items():
        if parent == "__root__":
            continue
        assert len(children) == 1, (
            f"Branch detected: revision {parent!r} has multiple children: "
            f"{children} (files: {[revisions[c] for c in children]})"
        )
