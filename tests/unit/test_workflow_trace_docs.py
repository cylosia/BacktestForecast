from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_TRACE_DOC = REPO_ROOT / "docs" / "workflow-trace.md"
RUNBOOK_DOC = REPO_ROOT / "docs" / "RUNBOOK.md"
DISPATCH_MODULE = REPO_ROOT / "apps" / "api" / "app" / "dispatch.py"
DEPENDENCIES_MODULE = REPO_ROOT / "apps" / "api" / "app" / "dependencies.py"
META_ROUTER = REPO_ROOT / "apps" / "api" / "app" / "routers" / "meta.py"


def test_workflow_trace_doc_mentions_current_auth_and_dispatch_behavior() -> None:
    text = WORKFLOW_TRACE_DOC.read_text(encoding="utf-8")
    lowered = text.lower()

    assert "__session" in text
    assert "maintenance.poll_outbox" in text
    assert "degrades by returning unauthenticated metadata" in lowered
    assert "postgresql `content_bytes`" in text
    assert "billing webhook events also have a fallback audit persistence path" in lowered


def test_runbook_mentions_meta_degradation_and_outbox_diagnosis() -> None:
    text = RUNBOOK_DOC.read_text(encoding="utf-8")
    lowered = text.lower()

    assert "/v1/meta" in text
    assert "degrades to unauthenticated metadata" in lowered
    assert "transactional outbox path" in lowered
    assert "content_bytes" in text
    assert "billing.audit_write_failed" in text


def test_workflow_docs_match_runtime_code_clues() -> None:
    dispatch_source = DISPATCH_MODULE.read_text(encoding="utf-8")
    dependencies_source = DEPENDENCIES_MODULE.read_text(encoding="utf-8")
    meta_source = META_ROUTER.read_text(encoding="utf-8")

    assert "poll_outbox" in dispatch_source
    assert "__session" in dependencies_source
    assert "X-Requested-With" in dependencies_source
    assert "meta.auth_degraded_db_unavailable" in meta_source
