"""Validate operational docs against current runtime configuration."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for candidate in (PROJECT_ROOT, PROJECT_ROOT / "src"):
    candidate_str = str(candidate)
    if candidate_str not in sys.path:
        sys.path.insert(0, candidate_str)

from backtestforecast.docs_invariants import validate_operational_docs


def main() -> int:
    errors = validate_operational_docs(PROJECT_ROOT)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Operational docs OK: current docs/navigation/runtime invariants are aligned.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
