"""Verify the Alembic migration chain converges to a single head."""
from __future__ import annotations

import ast
import pathlib


def _parse_revision_fields(text: str) -> tuple[str | None, list[str]]:
    """Extract revision and down_revision(s) from a migration file.

    Handles both ``revision = "..."`` and ``revision: str = "..."`` forms,
    and both string and tuple down_revision values.
    """
    rev = None
    down_revs: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("revision") and "=" in stripped:
            if "down_revision" in stripped:
                continue
            val = stripped.split("=", 1)[1].strip()
            try:
                rev = ast.literal_eval(val)
            except (ValueError, SyntaxError):
                rev = val.strip('"').strip("'")
        elif stripped.startswith("down_revision") and "=" in stripped:
            val = stripped.split("=", 1)[1].strip()
            try:
                parsed = ast.literal_eval(val)
            except (ValueError, SyntaxError):
                parsed = val.strip('"').strip("'")
            if parsed is None:
                pass
            elif isinstance(parsed, str):
                down_revs.append(parsed)
            elif isinstance(parsed, (tuple, list)):
                down_revs.extend(str(v) for v in parsed if v is not None)
    return rev, down_revs


def test_migration_chain_has_single_head():
    """The chain must converge to a single head.

    Merge migrations (tuple down_revision) are valid — they deliberately
    join two branches into one.  The test verifies that all branches are
    ultimately merged so only one head exists.
    """
    versions_dir = pathlib.Path(__file__).resolve().parents[2] / "alembic" / "versions"
    revisions: dict[str, str] = {}
    children_of: dict[str, list[str]] = {}

    for py_file in sorted(versions_dir.glob("*.py")):
        if py_file.name == "__init__.py":
            continue
        text = py_file.read_text()
        rev, down_revs = _parse_revision_fields(text)
        if rev is None:
            continue
        assert rev not in revisions, (
            f"Duplicate revision {rev!r} in {py_file.name} and {revisions[rev]}"
        )
        revisions[rev] = py_file.name
        for dr in down_revs:
            children_of.setdefault(dr, []).append(rev)
        if not down_revs:
            children_of.setdefault("__root__", []).append(rev)

    all_revs = set(revisions.keys())
    is_parent = set()
    for parent, kids in children_of.items():
        if parent != "__root__":
            is_parent.add(parent)
    heads = all_revs - is_parent
    assert len(heads) == 1, (
        f"Expected exactly 1 head, found {len(heads)}: "
        f"{[(h, revisions.get(h, '?')) for h in sorted(heads)]}"
    )
