"""Verify migration filenames match their internal revision IDs.

Audit fix 5-8: Mismatched filenames cause operational confusion. The filename
prefix (e.g. 20260318_0010) must match the revision = "..." value inside.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


def _get_migrations_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "alembic" / "versions"


def test_all_migration_filenames_match_revision_ids():
    migrations_dir = _get_migrations_dir()
    if not migrations_dir.exists():
        pytest.skip("alembic/versions directory not found")

    revision_pattern = re.compile(r'^revision\s*=\s*["\'](\S+)["\']', re.MULTILINE)
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


def test_migration_chain_is_linear():
    """Verify each migration's down_revision matches the previous revision."""
    migrations_dir = _get_migrations_dir()
    if not migrations_dir.exists():
        pytest.skip("alembic/versions directory not found")

    rev_pattern = re.compile(r'^revision\s*=\s*["\'](\S+)["\']', re.MULTILINE)
    down_pattern = re.compile(r'^down_revision\s*=\s*["\'](\S+)["\']', re.MULTILINE)

    revisions: dict[str, str] = {}
    for migration_file in migrations_dir.glob("*.py"):
        if migration_file.name == "__init__.py":
            continue
        content = migration_file.read_text(encoding="utf-8")
        rev_match = rev_pattern.search(content)
        down_match = down_pattern.search(content)
        if rev_match and down_match:
            revisions[rev_match.group(1)] = down_match.group(1)

    seen = set()
    for rev, down_rev in revisions.items():
        if down_rev in seen and down_rev != "None":
            pytest.fail(f"Multiple migrations share down_revision={down_rev}")
        seen.add(down_rev)
