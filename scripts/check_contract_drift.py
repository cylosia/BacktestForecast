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
for candidate in (PROJECT_ROOT, PROJECT_ROOT / "src"):
    candidate_str = str(candidate)
    if candidate_str not in sys.path:
        sys.path.insert(0, candidate_str)
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


def check_sweep_types() -> int:
    """Verify manually-maintained sweep TypeScript types include all backend fields."""
    from backtestforecast.schemas.sweeps import SweepJobResponse, SweepResultResponse

    ts_path = PROJECT_ROOT / "packages" / "api-client" / "src" / "index.ts"
    if not ts_path.exists():
        print(f"ERROR: {ts_path} not found", file=sys.stderr)
        return 1

    ts_content = ts_path.read_text(encoding="utf-8")
    errors: list[str] = []

    sweep_job_fields = set(SweepJobResponse.model_fields.keys())
    sweep_result_fields = set(SweepResultResponse.model_fields.keys())

    for field_name in sweep_job_fields:
        alias = SweepJobResponse.model_fields[field_name].alias
        ts_name = alias if alias else field_name
        if ts_name not in ts_content and field_name not in ts_content:
            errors.append(f"SweepJobResponse.{field_name} (ts: {ts_name}) missing from TypeScript")

    for field_name in sweep_result_fields:
        alias = SweepResultResponse.model_fields[field_name].alias
        ts_name = alias if alias else field_name
        if ts_name not in ts_content and field_name not in ts_content:
            errors.append(f"SweepResultResponse.{field_name} (ts: {ts_name}) missing from TypeScript")

    if errors:
        print("Sweep TypeScript type drift detected:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    print("Sweep types OK: all backend fields present in TypeScript.")
    return 0


if __name__ == "__main__":
    code = main()
    code = max(code, check_sweep_types())
    raise SystemExit(code)
