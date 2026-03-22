#!/usr/bin/env python3
"""Fail if app pages that consume API payloads introduce TypeScript `any`.

Scope is intentionally narrow: only Next.js app `page.tsx` files that import
server API helpers or generated API client types. This avoids flagging prose
strings like "analyze any symbol" while still protecting the page layer that
renders backend payloads.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_web_env=True)
APP_DIR = ROOT / "apps" / "web" / "app" / "app"

PAGE_GLOBS = ("**/page.tsx",)
API_IMPORT_MARKERS = ('@/lib/api/server', "@backtestforecast/api-client")
ANY_PATTERNS = (
    re.compile(r"\bas\s+any\b"),
    re.compile(r":\s*any\b"),
    re.compile(r"<\s*any\s*>"),
    re.compile(r"\bPromise<\s*any\s*>"),
    re.compile(r"\bArray<\s*any\s*>"),
    re.compile(r"\bRecord<[^>]*\bany\b"),
)


def iter_page_files() -> list[Path]:
    files: list[Path] = []
    for pattern in PAGE_GLOBS:
        files.extend(APP_DIR.glob(pattern))
    return sorted(files)


def main() -> int:
    failures: list[str] = []
    for path in iter_page_files():
        text = path.read_text()
        if not any(marker in text for marker in API_IMPORT_MARKERS):
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            if any(pattern.search(line) for pattern in ANY_PATTERNS):
                failures.append(f"{path.relative_to(ROOT)}:{lineno}: disallowed `any` in API-backed app page")

    if failures:
        print("Disallowed `any` usage found in API-backed app pages:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    print("No disallowed `any` usage found in API-backed app pages.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
