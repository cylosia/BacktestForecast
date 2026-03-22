from __future__ import annotations

from pathlib import Path

from backtestforecast.docs_invariants import validate_operational_docs

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_INDEX = REPO_ROOT / "docs" / "README.md"
WORKFLOW_TRACE = REPO_ROOT / "docs" / "workflow-trace.md"
ARCHIVE_AUDIT_LOG = REPO_ROOT / "docs" / "archive" / "audit-log.md"
DOCS_CHECK_SCRIPT = REPO_ROOT / "scripts" / "check_operational_docs.py"


def test_operational_docs_invariants_match_runtime() -> None:
    errors = validate_operational_docs(REPO_ROOT)
    assert errors == []


def test_docs_index_routes_historical_docs_to_archive() -> None:
    text = DOCS_INDEX.read_text(encoding="utf-8")
    assert "Historical audit archives" in text
    assert "docs/archive/" in text
    assert "Subsystem ownership map" in text


def test_workflow_trace_uses_explicit_runtime_ownership() -> None:
    text = WORKFLOW_TRACE.read_text(encoding="utf-8")
    assert "Primary owner: API + worker maintainers" in text
    assert "Backup owner: Active on-call rotation" in text
    assert "pricing contract fetch" in text


def test_archive_log_is_marked_historical_and_script_exists() -> None:
    text = ARCHIVE_AUDIT_LOG.read_text(encoding="utf-8").lower()
    assert "historical snapshot" in text
    assert "not be used as the source of truth" in text
    assert DOCS_CHECK_SCRIPT.exists()
