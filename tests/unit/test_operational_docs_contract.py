from __future__ import annotations

import re
from pathlib import Path

from apps.worker.app.celery_app import celery_app
from backtestforecast.schemas.backtests import CreateBacktestRunRequest

REPO_ROOT = Path(__file__).resolve().parents[2]
KNOWN_LIMITATIONS = REPO_ROOT / "docs" / "known-limitations.md"
WORKFLOW_TRACE = REPO_ROOT / "docs" / "workflow-trace.md"
RUNBOOK = REPO_ROOT / "docs" / "RUNBOOK.md"
AUDIT_LOG = REPO_ROOT / "docs" / "audit-log.md"
DOCS_INDEX = REPO_ROOT / "docs" / "README.md"
DOCS_CHECK_SCRIPT = REPO_ROOT / "scripts" / "check_operational_docs.py"
WEB_VALIDATION_CONSTANTS = REPO_ROOT / "apps" / "web" / "lib" / "validation-constants.ts"


def test_workflow_trace_is_authoritative_current_state_doc() -> None:
    text = WORKFLOW_TRACE.read_text(encoding="utf-8")
    lowered = text.lower()
    assert "authoritative current-state document" in lowered
    assert "**owners**" in lowered
    assert "primary owner: api + worker maintainers" in lowered
    assert "backup owner: active on-call rotation" in lowered
    assert "review cadence:" in lowered


def test_related_docs_point_to_authoritative_operational_page() -> None:
    workflow_name = "docs/workflow-trace.md"
    assert workflow_name in KNOWN_LIMITATIONS.read_text(encoding="utf-8")
    assert workflow_name in RUNBOOK.read_text(encoding="utf-8")
    audit_text = AUDIT_LOG.read_text(encoding="utf-8").lower()
    docs_index = DOCS_INDEX.read_text(encoding="utf-8")
    assert "historical archive" in audit_text
    assert "should not be used as the source of truth" in audit_text
    assert "Historical audit archives" in docs_index
    assert "docs/audit/" in docs_index
    assert DOCS_CHECK_SCRIPT.exists()


def test_outbox_docs_align_with_live_scheduler() -> None:
    beat_schedule = celery_app.conf.beat_schedule
    assert "poll-outbox" in beat_schedule
    assert beat_schedule["poll-outbox"]["task"] == "maintenance.poll_outbox"

    docs = "\n".join([
        WORKFLOW_TRACE.read_text(encoding="utf-8"),
        KNOWN_LIMITATIONS.read_text(encoding="utf-8"),
        RUNBOOK.read_text(encoding="utf-8"),
    ]).lower()
    assert "poll_outbox" in docs
    assert "disabled" not in docs
    assert "scaffolding only" not in docs
    if "commit-first gap" in docs:
        assert "no longer evidence of a" in docs


def test_target_dte_docs_match_frontend_and_backend_validation() -> None:
    workflow_text = WORKFLOW_TRACE.read_text(encoding="utf-8").lower()
    known_text = KNOWN_LIMITATIONS.read_text(encoding="utf-8").lower()
    combined = f"{workflow_text}\n{known_text}"
    assert "docs/readme.md" in combined
    assert "target_dte >= 7" not in combined
    assert "frontend/backend schema mismatch" not in combined

    constants_text = WEB_VALIDATION_CONSTANTS.read_text(encoding="utf-8")
    match = re.search(r"export const TARGET_DTE_MIN = (\d+);", constants_text)
    assert match is not None, "TARGET_DTE_MIN must be defined in web validation constants"

    frontend_min = int(match.group(1))
    field = CreateBacktestRunRequest.model_fields["target_dte"]
    backend_min = None
    for metadata in field.metadata:
        ge = getattr(metadata, "ge", None)
        if ge is not None:
            backend_min = int(ge)
            break

    assert backend_min == 1
    assert frontend_min == backend_min
