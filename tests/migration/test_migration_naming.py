"""Verify migration files follow naming conventions."""
from __future__ import annotations

import re
from pathlib import Path

VERSIONS_DIR = Path(__file__).resolve().parents[2] / "alembic" / "versions"
PATTERN = re.compile(r"^\d{8}_\d{4}_[a-z0-9_]+\.py$")


def test_all_migrations_follow_naming_convention():
    """Migration filenames must match YYYYMMDD_HHMM_description.py."""
    violations = []
    for path in sorted(VERSIONS_DIR.glob("*.py")):
        if path.name == "__init__.py":
            continue
        if not PATTERN.match(path.name):
            violations.append(path.name)
    assert not violations, (
        f"Migration files with non-standard names: {violations}\n"
        f"Expected format: YYYYMMDD_HHMM_description.py"
    )


def test_no_duplicate_revision_ids():
    """Each migration must have a unique revision ID."""
    revision_re = re.compile(r'^revision\s*=\s*["\']([^"\']+)["\']', re.MULTILINE)
    revisions: dict[str, str] = {}
    duplicates = []
    for path in sorted(VERSIONS_DIR.glob("*.py")):
        if path.name == "__init__.py":
            continue
        content = path.read_text(encoding="utf-8")
        match = revision_re.search(content)
        if match:
            rev_id = match.group(1)
            if rev_id in revisions:
                duplicates.append((rev_id, revisions[rev_id], path.name))
            else:
                revisions[rev_id] = path.name
    assert not duplicates, f"Duplicate revision IDs found: {duplicates}"
