from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from backtestforecast.config import Settings

_SECURITY_CRITICAL_VARS = {
    "REQUEST_MAX_BODY_BYTES",
    "REQUEST_TIMEOUT_SECONDS",
    "RATE_LIMIT_FAIL_CLOSED",
    "MASSIVE_TIMEOUT_SECONDS",
    "DB_POOL_TIMEOUT",
    "SCAN_TIMEOUT_SECONDS",
    "SWEEP_TIMEOUT_SECONDS",
    "SSE_REDIS_SOCKET_TIMEOUT",
    "SSE_REDIS_CONNECT_TIMEOUT",
}

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DOCS_PATH = _REPO_ROOT / "docs" / "env-vars.md"
_ENV_EXAMPLE_PATHS = [
    _REPO_ROOT / ".env.example",
    _REPO_ROOT / "apps" / "api" / ".env.example",
]


def _settings_defaults() -> dict[str, object]:
    settings = Settings(_env_file=None)
    return {
        "REQUEST_MAX_BODY_BYTES": settings.request_max_body_bytes,
        "REQUEST_TIMEOUT_SECONDS": settings.request_timeout_seconds,
        "RATE_LIMIT_FAIL_CLOSED": settings.rate_limit_fail_closed,
        "MASSIVE_TIMEOUT_SECONDS": settings.massive_timeout_seconds,
        "DB_POOL_TIMEOUT": settings.db_pool_timeout,
        "SCAN_TIMEOUT_SECONDS": settings.scan_timeout_seconds,
        "SWEEP_TIMEOUT_SECONDS": settings.sweep_timeout_seconds,
        "SSE_REDIS_SOCKET_TIMEOUT": settings.sse_redis_socket_timeout,
        "SSE_REDIS_CONNECT_TIMEOUT": settings.sse_redis_connect_timeout,
    }


def _parse_env_example(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in _SECURITY_CRITICAL_VARS:
            continue
        values[key] = value.split("#", 1)[0].strip()
    return values


def _parse_docs_defaults(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line.startswith("| `"):
            continue
        columns = [column.strip() for column in line.strip("|").split("|")]
        if not columns:
            continue
        variable = columns[0].strip("`")
        if variable not in _SECURITY_CRITICAL_VARS:
            continue
        default_index = 1 if len(columns) == 3 else 2
        values[variable] = columns[default_index].strip("`")
    return values


def _coerce_documented_value(raw_value: str, default: object) -> object:
    if isinstance(default, bool):
        normalized = raw_value.strip().lower()
        if normalized not in {"true", "false"}:
            raise AssertionError(f"Expected a boolean literal, got: {raw_value!r}")
        return normalized == "true"
    if isinstance(default, int) and not isinstance(default, bool):
        return int(raw_value)
    if isinstance(default, float):
        return float(Decimal(raw_value))
    return raw_value


def test_env_examples_match_security_critical_settings_defaults() -> None:
    defaults = _settings_defaults()

    for path in _ENV_EXAMPLE_PATHS:
        documented = _parse_env_example(path)
        missing = sorted(_SECURITY_CRITICAL_VARS - documented.keys())
        assert not missing, f"{path.relative_to(_REPO_ROOT)} is missing defaults for: {missing}"

        for variable, default in defaults.items():
            assert _coerce_documented_value(documented[variable], default) == default, (
                f"{path.relative_to(_REPO_ROOT)} documents {variable}={documented[variable]!r}, "
                f"but Settings defaults to {default!r}"
            )


def test_docs_match_security_critical_settings_defaults() -> None:
    defaults = _settings_defaults()
    documented = _parse_docs_defaults(_DOCS_PATH)
    missing = sorted(_SECURITY_CRITICAL_VARS - documented.keys())
    assert not missing, f"docs/env-vars.md is missing defaults for: {missing}"

    for variable, default in defaults.items():
        assert _coerce_documented_value(documented[variable], default) == default, (
            f"docs/env-vars.md documents {variable}={documented[variable]!r}, "
            f"but Settings defaults to {default!r}"
        )
