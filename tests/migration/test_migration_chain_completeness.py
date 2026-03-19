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

    def test_down_revision_chain_is_connected(self):
        """Each down_revision must point to existing revision(s) (except root).

        Handles both string and tuple down_revision values (merge migrations).
        Handles type-annotated assignments like ``revision: str = "..."``.
        """
        import ast
        from pathlib import Path

        versions_dir = Path(__file__).resolve().parents[2] / "alembic" / "versions"
        revisions: dict[str, str] = {}
        down_map: dict[str, list[str]] = {}

        for py_file in sorted(versions_dir.glob("*.py")):
            if py_file.name == "__init__.py":
                continue
            content = py_file.read_text(encoding="utf-8")
            rev_match = re.search(r'^revision[\s:]*(?:str\s*)?=\s*["\'](.+?)["\']', content, re.MULTILINE)
            down_match = re.search(r'^down_revision[\s:]*(?:str\s*)?=\s*(.+?)$', content, re.MULTILINE)
            if rev_match:
                rev_id = rev_match.group(1)
                revisions[rev_id] = py_file.name
                if down_match:
                    raw = down_match.group(1).strip()
                    try:
                        parsed = ast.literal_eval(raw)
                    except (ValueError, SyntaxError):
                        parsed = raw.strip("\"'")
                    if parsed is None:
                        down_map[rev_id] = []
                    elif isinstance(parsed, str):
                        down_map[rev_id] = [parsed]
                    elif isinstance(parsed, (tuple, list)):
                        down_map[rev_id] = [str(v) for v in parsed if v is not None]
                    else:
                        down_map[rev_id] = []

        roots = [r for r, deps in down_map.items() if not deps]
        assert len(roots) == 1, f"Expected exactly one root migration, found {len(roots)}: {roots}"

        for rev_id, deps in down_map.items():
            for dep in deps:
                assert dep in revisions, (
                    f"Migration {revisions[rev_id]} has down_revision='{dep}' "
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
