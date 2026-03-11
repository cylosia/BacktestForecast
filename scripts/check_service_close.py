"""CI check: classes that instantiate BacktestExecutionService must define close().

Scans all Python files under src/ and apps/ for classes that call
BacktestExecutionService() and verifies they also define a close() method.
Exits with code 1 if any violations are found.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

TARGET = "BacktestExecutionService"
SEARCH_DIRS = [Path("src"), Path("apps")]


def check_file(path: Path) -> list[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []

    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue

        creates_target = False
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                func = child.func
                name = getattr(func, "id", None) or getattr(func, "attr", None)
                if name == TARGET:
                    creates_target = True
                    break

        if not creates_target:
            continue

        has_close = any(
            isinstance(item, ast.FunctionDef) and item.name == "close"
            for item in node.body
        )
        if not has_close:
            violations.append(f"  {path}:{node.lineno} - class {node.name}")

    return violations


def main() -> int:
    all_violations: list[str] = []
    for search_dir in SEARCH_DIRS:
        if not search_dir.exists():
            continue
        for py_file in search_dir.rglob("*.py"):
            all_violations.extend(check_file(py_file))

    if all_violations:
        print(f"FAIL: {len(all_violations)} class(es) create {TARGET} without defining close():\n")
        for v in all_violations:
            print(v)
        print(f"\nEvery class that instantiates {TARGET} must define a close() method.")
        return 1

    print(f"OK: all classes that instantiate {TARGET} define close().")
    return 0


if __name__ == "__main__":
    sys.exit(main())
