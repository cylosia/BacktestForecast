from __future__ import annotations

from pathlib import Path


def _read(path: str) -> str:
    return Path(path).read_text()


def test_webhook_unhandled_errors_raise_http_exception_not_custom_json() -> None:
    source = _read("apps/api/app/routers/billing.py")
    assert "webhook_processing_failed" in source
    assert '"received": False' not in source
    assert "JSONResponse" not in source


def test_layout_does_not_swallow_current_user_bootstrap_failure() -> None:
    source = _read("apps/web/app/app/layout.tsx")
    assert "await getCurrentUser()" in source
    assert "Allow the app shell to render" not in source


def test_datetime_utc_compatibility_helper_avoids_python_312_only_imports() -> None:
    helper = _read("src/backtestforecast/time.py")
    assert 'getattr(_datetime, "UTC", _datetime.timezone.utc)' in helper
    for path in (
        "src/backtestforecast/services/backtests.py",
        "src/backtestforecast/services/daily_picks.py",
        "src/backtestforecast/pipeline/service.py",
        "src/backtestforecast/management/backfill_metrics.py",
        "apps/api/app/routers/health.py",
    ):
        source = _read(path)
        assert "from datetime import UTC" not in source, path
        assert "from backtestforecast.time import UTC" in source or "from backtestforecast.time import UTC" in source.replace("            ", ""), path


def test_pytest_asyncio_mode_option_is_registered_locally() -> None:
    source = _read("tests/conftest.py")
    assert 'parser.addini("asyncio_mode"' in source
    assert 'parser.addini("timeout"' in source
