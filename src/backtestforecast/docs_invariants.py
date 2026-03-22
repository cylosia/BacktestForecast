from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

from apps.worker.app.celery_app import celery_app
from backtestforecast.schemas.backtests import CreateBacktestRunRequest


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def validate_operational_docs(project_root: Path) -> list[str]:
    docs_index = project_root / "docs" / "README.md"
    workflow_trace = project_root / "docs" / "workflow-trace.md"
    runbook = project_root / "docs" / "RUNBOOK.md"
    known_limitations = project_root / "docs" / "known-limitations.md"
    audit_log = project_root / "docs" / "archive" / "audit-log.md"
    web_validation_constants = project_root / "apps" / "web" / "lib" / "validation-constants.ts"

    workflow = _read(workflow_trace)
    runbook_text = _read(runbook)
    known = _read(known_limitations)
    audit_log_text = _read(audit_log)
    docs_index_text = _read(docs_index)
    docs_combined = "\n".join([workflow, runbook_text, known]).lower()

    errors: list[str] = []

    def expect(condition: bool, message: str) -> None:
        if not condition:
            errors.append(message)

    expect("authoritative current-state document" in workflow.lower(), "workflow trace must declare itself authoritative")
    expect("Primary owner: API + worker maintainers" in workflow, "workflow trace must use explicit API/worker ownership")
    expect("Backup owner: Active on-call rotation" in workflow, "workflow trace must identify the on-call backup owner")
    expect("docs/README.md" in runbook_text, "runbook must link to docs index")
    expect("docs/README.md" in known, "known limitations must link to docs index")
    expect("Historical audit archives" in docs_index_text, "docs index must label historical audit archives")
    expect("docs/archive/" in docs_index_text, "docs index must route historical docs to docs/archive/")
    expect("Subsystem ownership map" in docs_index_text, "docs index must include subsystem ownership map")
    expect("historical snapshot" in audit_log_text.lower(), "archived audit log must identify itself as historical")
    expect("not be used as the source of truth" in audit_log_text.lower(), "archived audit log must disclaim current authority")

    beat_schedule = celery_app.conf.beat_schedule
    expect("poll-outbox" in beat_schedule, "celery beat schedule must include poll-outbox")
    expect(beat_schedule.get("poll-outbox", {}).get("task") == "maintenance.poll_outbox", "poll-outbox must route to maintenance.poll_outbox")
    expect("poll_outbox" in docs_combined, "current docs must mention poll_outbox")
    expect("disabled" not in docs_combined, "current docs must not say poll_outbox is disabled")
    expect("scaffolding only" not in docs_combined, "current docs must not say outbox is scaffolding only")
    expect("target_dte >= 7" not in docs_combined, "current docs must not claim stale target_dte >= 7 behavior")
    expect("frontend/backend schema mismatch" not in docs_combined, "current docs must not claim stale frontend/backend schema mismatch")
    expect("hardcoded pricing/ui assumptions" not in docs_combined, "current docs must not describe pricing UI as hardcoded")

    match = re.search(r"export const TARGET_DTE_MIN = (\d+);", _read(web_validation_constants))
    expect(match is not None, "frontend validation constants must define TARGET_DTE_MIN")
    if match is not None:
        frontend_min = int(match.group(1))
        field = CreateBacktestRunRequest.model_fields["target_dte"]
        backend_min = None
        for metadata in field.metadata:
            ge = getattr(metadata, "ge", None)
            if ge is not None:
                backend_min = int(ge)
                break
        expect(backend_min == 1, "backend target_dte minimum must remain 1")
        expect(frontend_min == backend_min, "frontend TARGET_DTE_MIN must match backend target_dte minimum")

    return errors
