from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FORBIDDEN_FILES = [
    ROOT / "openapi.snapshot.json.new",
]
FORBIDDEN_GLOBS = [
    "pytest-cache-files-*",
]
CONTRACT_FILES = [
    "openapi.snapshot.json",
    "packages/api-client/src/schema.d.ts",
]


def _fail(message: str) -> int:
    print(message, file=sys.stderr)
    return 1


def main() -> int:
    for path in FORBIDDEN_FILES:
        if path.exists():
            return _fail(f"Forbidden generated artifact present: {path.relative_to(ROOT)}")

    for pattern in FORBIDDEN_GLOBS:
        matches = sorted(path.relative_to(ROOT) for path in ROOT.glob(pattern))
        if matches:
            joined = ", ".join(str(match) for match in matches)
            return _fail(f"Forbidden generated artifact(s) present: {joined}")

    diff_result = subprocess.run(
        ["git", "diff", "--exit-code", "--", *CONTRACT_FILES],
        cwd=ROOT,
        check=False,
    )
    if diff_result.returncode != 0:
        return _fail(
            "Generated contract files are dirty after validation. "
            "Regenerate and commit the intended snapshot/type changes.",
        )

    cached_diff_result = subprocess.run(
        ["git", "diff", "--cached", "--exit-code", "--", *CONTRACT_FILES],
        cwd=ROOT,
        check=False,
    )
    if cached_diff_result.returncode != 0:
        return _fail(
            "Generated contract files have staged but unverified changes. "
            "Regenerate cleanly or commit the intended snapshot/type updates.",
        )

    print("Generated artifact hygiene checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
