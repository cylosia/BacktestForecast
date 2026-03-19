"""Check that the committed OpenAPI snapshot matches the live FastAPI schema.

Exit 0 if identical, exit 1 with a diff if drifted.

This script is executed in the CI pipeline (.github/workflows/ci.yml) as part
of the "backend-and-web" job under the "Check OpenAPI schema drift" step.
Any changes to FastAPI route signatures or Pydantic response models will cause
this check to fail until the snapshot is regenerated with:
    python scripts/export_openapi.py > openapi.snapshot.json

Usage:
    python scripts/check_openapi_drift.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SNAPSHOT_PATH = Path(__file__).resolve().parent.parent / "openapi.snapshot.json"


def main() -> int:
    from apps.api.app.main import app

    live = json.dumps(app.openapi(), indent=2, sort_keys=True) + "\n"

    if not SNAPSHOT_PATH.exists():
        print(f"ERROR: snapshot not found at {SNAPSHOT_PATH}", file=sys.stderr)
        print("Run:  python scripts/export_openapi.py > openapi.snapshot.json", file=sys.stderr)
        return 1

    committed = SNAPSHOT_PATH.read_text(encoding="utf-8")

    if live == committed:
        print("OpenAPI schema matches committed snapshot.")
        return 0

    import difflib

    diff = difflib.unified_diff(
        committed.splitlines(keepends=True),
        live.splitlines(keepends=True),
        fromfile="openapi.snapshot.json (committed)",
        tofile="openapi.snapshot.json (live)",
    )
    sys.stdout.writelines(diff)
    print(
        "\nERROR: OpenAPI schema has drifted from committed snapshot.",
        file=sys.stderr,
    )
    print(
        "Regenerate with:  python scripts/export_openapi.py > openapi.snapshot.json",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
