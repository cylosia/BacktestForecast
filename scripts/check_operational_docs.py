"""Validate operational docs against current runtime configuration.

This script is designed for direct use in CI. It verifies that current docs:
- designate the authoritative operational assumptions page,
- clearly separate current docs from historical audit archives,
- keep outbox documentation aligned with the live Celery beat schedule,
- and keep `target_dte` contract notes aligned with frontend/backend validation.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for candidate in (PROJECT_ROOT, PROJECT_ROOT / "src"):
    candidate_str = str(candidate)
    if candidate_str not in sys.path:
        sys.path.insert(0, candidate_str)

from apps.worker.app.celery_app import celery_app
from backtestforecast.schemas.backtests import CreateBacktestRunRequest

DOCS_INDEX = PROJECT_ROOT / "docs" / "README.md"
WORKFLOW_TRACE = PROJECT_ROOT / "docs" / "workflow-trace.md"
RUNBOOK = PROJECT_ROOT / "docs" / "RUNBOOK.md"
KNOWN_LIMITATIONS = PROJECT_ROOT / "docs" / "known-limitations.md"
AUDIT_LOG = PROJECT_ROOT / "docs" / "audit-log.md"
WEB_VALIDATION_CONSTANTS = PROJECT_ROOT / "apps" / "web" / "lib" / "validation-constants.ts"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def main() -> int:
    workflow = _read(WORKFLOW_TRACE)
    runbook = _read(RUNBOOK)
    known = _read(KNOWN_LIMITATIONS)
    audit_log = _read(AUDIT_LOG)
    docs_index = _read(DOCS_INDEX)
    docs_combined = "\n".join([workflow, runbook, known]).lower()

    assert "authoritative current-state document" in workflow.lower()
    assert "Primary owner: API + worker maintainers" in workflow
    assert "Backup owner: Active on-call rotation" in workflow
    assert "docs/README.md" in runbook
    assert "docs/README.md" in known
    assert "Historical audit archives" in docs_index
    assert "docs/audit/" in docs_index
    assert "historical snapshot" in audit_log.lower()
    assert "not be used as the source of truth" in audit_log.lower()

    beat_schedule = celery_app.conf.beat_schedule
    assert "poll-outbox" in beat_schedule
    assert beat_schedule["poll-outbox"]["task"] == "maintenance.poll_outbox"
    assert "poll_outbox" in docs_combined
    assert "disabled" not in docs_combined
    assert "scaffolding only" not in docs_combined
    assert "target_dte >= 7" not in docs_combined
    assert "frontend/backend schema mismatch" not in docs_combined

    match = re.search(r"export const TARGET_DTE_MIN = (\d+);", _read(WEB_VALIDATION_CONSTANTS))
    assert match is not None
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
    print("Operational docs OK: current docs/navigation/runtime invariants are aligned.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
