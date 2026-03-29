from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_env_file(path: Path, *, override: bool = False, protected_keys: set[str] | None = None) -> None:
    if not path.exists():
        return
    protected = protected_keys or set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key in protected:
            continue
        if not override:
            os.environ.setdefault(key, value)
            continue
        current = os.environ.get(key)
        if value or current in (None, ""):
            os.environ[key] = value


def bootstrap_repo(*, load_api_env: bool = False, load_web_env: bool = False) -> Path:
    protected_keys = set(os.environ)
    _load_env_file(ROOT / ".env", protected_keys=protected_keys)
    _load_env_file(ROOT / ".env.local", override=True, protected_keys=protected_keys)
    if load_api_env:
        _load_env_file(ROOT / "apps" / "api" / ".env.example", protected_keys=protected_keys)
        _load_env_file(ROOT / "apps" / "api" / ".env", override=True, protected_keys=protected_keys)
    if load_web_env:
        _load_env_file(ROOT / "apps" / "web" / ".env.example", protected_keys=protected_keys)
        _load_env_file(ROOT / "apps" / "web" / ".env.local", override=True, protected_keys=protected_keys)

    for path in (ROOT / "src", ROOT):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)
    return ROOT
