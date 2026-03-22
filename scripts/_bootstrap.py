from __future__ import annotations

import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def bootstrap_repo(*, load_api_env: bool = False, load_web_env: bool = False) -> Path:
    for candidate in (ROOT / ".env", ROOT / ".env.local"):
        _load_env_file(candidate)
    if load_api_env:
        for candidate in (ROOT / "apps" / "api" / ".env", ROOT / "apps" / "api" / ".env.example"):
            _load_env_file(candidate)
    if load_web_env:
        for candidate in (ROOT / "apps" / "web" / ".env.local", ROOT / "apps" / "web" / ".env.example"):
            _load_env_file(candidate)

    for path in (ROOT / "src", ROOT):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)
    return ROOT
