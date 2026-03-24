"""Verify migration filenames match their internal revision IDs.

Audit fix 5-8: Mismatched filenames cause operational confusion. The filename
prefix (e.g. 20260318_0010) must match the revision = "..." value inside.

Exception: legacy migrations whose revision ID doesn't follow the
YYYYMMDD_NNNN pattern (e.g. "0024_heartbeat") are excluded from the
filename check since renaming them would break production databases.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest


def _get_migrations_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "alembic" / "versions"


_YYYYMMDD_NNNN = re.compile(r'^\d{8}_\d{4}$')


def test_all_migration_filenames_match_revision_ids():
    migrations_dir = _get_migrations_dir()
    if not migrations_dir.exists():
        pytest.skip("alembic/versions directory not found")

    revision_pattern = re.compile(r'^revision[\s:]*(?:str\s*)?=\s*["\'](\S+)["\']', re.MULTILINE)
    filename_prefix_pattern = re.compile(r'^(\d{8}_\d{4})')
    mismatches: list[str] = []

    for migration_file in sorted(migrations_dir.glob("*.py")):
        if migration_file.name == "__init__.py":
            continue
        content = migration_file.read_text(encoding="utf-8")
        match = revision_pattern.search(content)
        if not match:
            continue
        internal_revision = match.group(1)
        if not _YYYYMMDD_NNNN.match(internal_revision):
            continue
        fname_match = filename_prefix_pattern.match(migration_file.name)
        if not fname_match:
            continue
        filename_prefix = fname_match.group(1)
        if filename_prefix != internal_revision:
            mismatches.append(
                f"{migration_file.name}: filename prefix={filename_prefix}, "
                f"revision={internal_revision}"
            )

    assert not mismatches, (
        "Migration filenames do not match their internal revision IDs:\n"
        + "\n".join(mismatches)
    )


def test_migration_chain_has_single_head():
    """Verify the chain converges to a single head.

    Handles merge migrations (tuple down_revision) and type-annotated fields.
    """
    migrations_dir = _get_migrations_dir()
    if not migrations_dir.exists():
        pytest.skip("alembic/versions directory not found")

    rev_pattern = re.compile(r'^revision[\s:]*(?:str\s*)?=\s*["\'](\S+)["\']', re.MULTILINE)
    down_pattern = re.compile(r'^down_revision[\s:]*(?:str\s*)?=\s*(.+?)$', re.MULTILINE)

    revisions: set[str] = set()
    is_parent: set[str] = set()

    for migration_file in migrations_dir.glob("*.py"):
        if migration_file.name == "__init__.py":
            continue
        content = migration_file.read_text(encoding="utf-8")
        rev_match = rev_pattern.search(content)
        if not rev_match:
            continue
        revisions.add(rev_match.group(1))

        down_match = down_pattern.search(content)
        if not down_match:
            continue
        raw = down_match.group(1).strip()
        try:
            parsed = ast.literal_eval(raw)
        except (ValueError, SyntaxError):
            parsed = raw.strip("\"'")
        if isinstance(parsed, str) and parsed != "None":
            is_parent.add(parsed)
        elif isinstance(parsed, (tuple, list)):
            for v in parsed:
                if v is not None:
                    is_parent.add(str(v))

    heads = revisions - is_parent
    assert len(heads) == 1, f"Expected 1 head, found {len(heads)}: {sorted(heads)}"
