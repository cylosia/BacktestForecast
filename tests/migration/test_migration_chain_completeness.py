"""Verify the migration chain is complete and consistent."""
from __future__ import annotations

import inspect
import re

import pytest


class TestMigrationChain:
    """Verify migration files form a valid linear chain."""

    def test_no_duplicate_revision_ids(self):
        """Each revision ID must be unique across all migration files."""
        from pathlib import Path

        versions_dir = Path(__file__).resolve().parents[2] / "alembic" / "versions"
        revisions: dict[str, str] = {}
        for py_file in sorted(versions_dir.glob("*.py")):
            if py_file.name == "__init__.py":
                continue
            content = py_file.read_text(encoding="utf-8")
            match = re.search(r'^revision\s*=\s*["\'](.+?)["\']', content, re.MULTILINE)
            if match:
                rev_id = match.group(1)
                assert rev_id not in revisions, (
                    f"Duplicate revision ID '{rev_id}' in {py_file.name} and {revisions[rev_id]}"
                )
                revisions[rev_id] = py_file.name

    def test_down_revision_chain_is_linear(self):
        """Each down_revision must point to exactly one existing revision (except root)."""
        from pathlib import Path

        versions_dir = Path(__file__).resolve().parents[2] / "alembic" / "versions"
        revisions: dict[str, str] = {}
        down_revisions: dict[str, str | None] = {}

        for py_file in sorted(versions_dir.glob("*.py")):
            if py_file.name == "__init__.py":
                continue
            content = py_file.read_text(encoding="utf-8")
            rev_match = re.search(r'^revision\s*=\s*["\'](.+?)["\']', content, re.MULTILINE)
            down_match = re.search(r'^down_revision\s*=\s*(.+?)$', content, re.MULTILINE)
            if rev_match:
                rev_id = rev_match.group(1)
                revisions[rev_id] = py_file.name
                if down_match:
                    raw = down_match.group(1).strip()
                    if raw == "None":
                        down_revisions[rev_id] = None
                    else:
                        down_revisions[rev_id] = raw.strip("\"'")

        roots = [r for r, d in down_revisions.items() if d is None]
        assert len(roots) == 1, f"Expected exactly one root migration, found {len(roots)}: {roots}"

        for rev_id, down_rev in down_revisions.items():
            if down_rev is not None:
                assert down_rev in revisions, (
                    f"Migration {revisions[rev_id]} has down_revision='{down_rev}' "
                    f"which doesn't exist in any migration file"
                )

    def test_trigger_creation_uses_drop_if_exists(self):
        """Migration 0010 must use DROP TRIGGER IF EXISTS before CREATE TRIGGER."""
        from pathlib import Path

        versions_dir = Path(__file__).resolve().parents[2] / "alembic" / "versions"
        target = versions_dir / "20260318_0010_schema_drift_fixes.py"
        if not target.exists():
            pytest.skip("Migration file not found")
        content = target.read_text(encoding="utf-8")
        assert "DROP TRIGGER IF EXISTS" in content, (
            "Migration 0010 must DROP TRIGGER IF EXISTS before CREATE TRIGGER "
            "to avoid failures when triggers already exist from earlier migrations"
        )
