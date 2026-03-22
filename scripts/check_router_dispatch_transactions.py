#!/usr/bin/env python3
"""Ensure routers do not manage DB transactions around service-level dispatch helpers."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import bootstrap_repo


ROOT = bootstrap_repo()
ROUTERS_DIR = ROOT / "apps" / "api" / "app" / "routers"
DISPATCH_MARKERS = (
    "create_and_dispatch(",
    "create_and_dispatch_job(",
    "create_and_dispatch_export(",
    "create_and_dispatch_analysis(",
)


def main() -> int:
    failures: list[str] = []
    for path in sorted(ROUTERS_DIR.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        if not any(marker in text for marker in DISPATCH_MARKERS):
            continue
        if "db.commit()" in text:
            failures.append(
                f"{path.relative_to(ROOT)}: routers using service-layer dispatch helpers must not call db.commit()"
            )
        if "dispatch_celery_task(" in text:
            failures.append(
                f"{path.relative_to(ROOT)}: routers must not call dispatch_celery_task() directly"
            )

    if failures:
        print("Router dispatch transaction checks failed:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    print("Router dispatch transaction checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
