from __future__ import annotations

import re
from pathlib import Path

from backtestforecast.schemas.backtests import CreateBacktestRunRequest

REPO_ROOT = Path(__file__).resolve().parents[2]
KNOWN_LIMITATIONS = REPO_ROOT / "docs" / "known-limitations.md"
WEB_VALIDATION_CONSTANTS = REPO_ROOT / "apps" / "web" / "lib" / "validation-constants.ts"


def test_target_dte_docs_do_not_claim_stale_frontend_backend_mismatch() -> None:
    text = KNOWN_LIMITATIONS.read_text(encoding="utf-8")
    lowered = text.lower()
    assert "frontend / api contract notes" not in lowered
    assert "target_dte >= 7" not in text
    assert "frontend/backend schema mismatch" not in lowered


def test_target_dte_frontend_constant_matches_backend_schema() -> None:
    text = WEB_VALIDATION_CONSTANTS.read_text(encoding="utf-8")
    match = re.search(r"export const TARGET_DTE_MIN = (\d+);", text)
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
