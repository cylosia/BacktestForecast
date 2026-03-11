"""Export the FastAPI OpenAPI schema to JSON.

Usage:
    python scripts/export_openapi.py            # prints to stdout
    python scripts/export_openapi.py > openapi.json
"""
from __future__ import annotations

import json
import sys


def main() -> None:
    from apps.api.app.main import app

    schema = app.openapi()
    json.dump(schema, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
