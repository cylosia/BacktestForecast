from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC_DIR = ROOT / "src"


def ensure_repo_import_paths() -> tuple[Path, Path]:
    """Keep this checkout's ``src`` tree ahead of stale editable installs."""

    desired = (SRC_DIR, ROOT)
    for path in desired:
        path_str = str(path)
        while path_str in sys.path:
            sys.path.remove(path_str)
    for path in reversed(desired):
        sys.path.insert(0, str(path))
    return ROOT, SRC_DIR
