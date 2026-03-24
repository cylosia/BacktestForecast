from pathlib import Path


def test_startup_logs_reloadable_and_restart_required_config_surfaces() -> None:
    source = Path("apps/api/app/main.py").read_text()
    assert "startup.config_reload_surfaces" in source
    assert "reloadable=[" in source
    assert "restart_required=[" in source


def test_invalidate_settings_docstring_clarifies_restart_boundaries() -> None:
    source = Path("src/backtestforecast/config.py").read_text()
    assert "process-local" in source
    assert "Startup-built surfaces still require a restart" in source or "still require a process restart" in source


def test_runtime_http_surfaces_are_dynamic_not_frozen() -> None:
    source = Path("src/backtestforecast/security/http.py").read_text()
    assert "Callable[[], int]" in source
    assert "app_env_resolver" in source
    assert "DynamicCORSMiddleware" in source
    assert "DynamicTrustedHostMiddleware" in source
