"""Contract test: verify that the generated frontend TypeScript types match the
live OpenAPI schema produced by the FastAPI backend.

This script ensures the auto-generated API client types (produced by
openapi-typescript) stay in sync with the backend.  It works by:

  1. Exporting the live OpenAPI JSON from the FastAPI app.
  2. Running openapi-typescript to regenerate the TypeScript definitions.
  3. Diffing the regenerated output against the committed file.

If there is a mismatch, the frontend is using stale types and the generated
client package should be regenerated:

    pnpm --filter @backtestforecast/api-client generate

Usage:
    python scripts/check_contract_drift.py

Prerequisites:
    - Node.js and pnpm installed
    - Python environment with the API app importable (PYTHONPATH=src:.)
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GENERATED_TYPES_PATH = PROJECT_ROOT / "packages" / "api-client" / "src" / "schema.d.ts"


def main() -> int:
    from apps.api.app.main import app

    schema_json = json.dumps(app.openapi(), indent=2, sort_keys=True)

    if not GENERATED_TYPES_PATH.exists():
        print(
            f"ERROR: generated types not found at {GENERATED_TYPES_PATH}",
            file=sys.stderr,
        )
        print(
            "Run:  pnpm --filter @backtestforecast/api-client generate",
            file=sys.stderr,
        )
        return 1

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as tmp:
        tmp.write(schema_json)
        tmp_path = tmp.name

    try:
        result = subprocess.run(
            ["pnpm", "exec", "openapi-typescript", tmp_path, "--output", "-"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        if result.returncode != 0:
            print(
                f"ERROR: openapi-typescript failed:\n{result.stderr}",
                file=sys.stderr,
            )
            return 1

        regenerated = result.stdout
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    committed = GENERATED_TYPES_PATH.read_text(encoding="utf-8")

    if regenerated.strip() == committed.strip():
        print("Contract OK: frontend TypeScript types match the OpenAPI schema.")
        return 0

    import difflib

    diff = difflib.unified_diff(
        committed.splitlines(keepends=True),
        regenerated.splitlines(keepends=True),
        fromfile="schema.d.ts (committed)",
        tofile="schema.d.ts (regenerated from live OpenAPI)",
    )
    sys.stdout.writelines(diff)
    print(
        "\nERROR: Frontend types have drifted from the OpenAPI schema.",
        file=sys.stderr,
    )
    print(
        "Regenerate with:  pnpm --filter @backtestforecast/api-client generate",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
