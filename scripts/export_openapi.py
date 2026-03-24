"""Export the FastAPI OpenAPI schema to JSON.

Usage:
    python scripts/export_openapi.py            # prints to stdout
    python scripts/export_openapi.py > openapi.json
"""
from __future__ import annotations

import json
import sys


def main() -> int:
    try:
        from apps.api.app.main import app
    except Exception as exc:
        print(f"ERROR: failed to import the FastAPI app: {exc}", file=sys.stderr)
        return 1

    try:
        schema = app.openapi()
    except Exception as exc:
        print(f"ERROR: failed to generate OpenAPI schema: {exc}", file=sys.stderr)
        return 1

    json.dump(schema, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
