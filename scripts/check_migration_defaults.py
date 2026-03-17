"""Pre-commit / CI check: every non-nullable column should have a server_default.

Reads all Alembic migration files in ``alembic/versions/`` and warns when a
column is declared with ``nullable=False`` but has no ``server_default``.
Primary-key and foreign-key columns are excluded since their values are
always supplied explicitly.

Exit code 1 if any violations are found.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

_VERSIONS_DIR = Path(__file__).resolve().parent.parent / "alembic" / "versions"

_COLUMN_RE = re.compile(
    r"""sa\.Column\((?P<args>(?:[^()]*|\([^()]*\))*)\)""",
    re.DOTALL,
)


def _check_file(path: Path) -> list[str]:
    content = path.read_text(encoding="utf-8")
    warnings: list[str] = []

    for match in _COLUMN_RE.finditer(content):
        args_text = match.group("args")

        if "primary_key=True" in args_text:
            continue
        if "ForeignKey(" in args_text or "sa.ForeignKey(" in args_text:
            continue

        if "nullable=False" not in args_text:
            continue
        if "server_default=" in args_text:
            continue

        name_match = re.match(r"""["']([^"']+)["']""", args_text.strip())
        col_name = name_match.group(1) if name_match else "<unknown>"
        lineno = content[: match.start()].count("\n") + 1
        warnings.append(f"  {path.name}:{lineno}  column {col_name!r} is nullable=False without server_default")

    return warnings


def main() -> int:
    if not _VERSIONS_DIR.is_dir():
        print(f"ERROR: versions directory not found: {_VERSIONS_DIR}", file=sys.stderr)
        return 1

    all_warnings: list[str] = []
    for migration in sorted(_VERSIONS_DIR.glob("*.py")):
        all_warnings.extend(_check_file(migration))

    if not all_warnings:
        print("OK — all non-nullable columns have server_default (excluding PKs and FKs).")
        return 0

    print(f"WARNING: {len(all_warnings)} column(s) with nullable=False but no server_default:")
    for w in all_warnings:
        print(w)
    return 1


if __name__ == "__main__":
    sys.exit(main())
