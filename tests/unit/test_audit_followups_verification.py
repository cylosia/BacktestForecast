from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (ROOT / relpath).read_text()


def test_use_polling_splits_terminal_resource_and_callback_status() -> None:
    source = _read("apps/web/hooks/use-polling.ts")
    assert "callbackStatus" in source
    assert "runTerminalPollingCallback" in source
    assert 'setStatus("done")' in source
    assert 'setCallbackStatus("running")' in source



def test_config_uses_structured_logging_for_missing_massive_api_key() -> None:
    source = _read("src/backtestforecast/config.py")
    assert "config.massive_api_key_missing" in source
    assert "logger.warning(" in source
    assert "warnings.warn(" not in source



def test_env_docs_describe_data_fetching_preconditions() -> None:
    source = _read("docs/env-vars.md")
    assert "Data-fetching feature preconditions" in source
    for feature in (
        "backtest creation",
        "scanner job creation",
        "sweep job creation",
        "symbol analysis creation",
    ):
        assert feature in source
    assert "MASSIVE_API_KEY" in source
    assert "EARNINGS_API_KEY" in source



def test_test_bootstrap_seeds_optional_provider_env_defaults() -> None:
    source = _read("tests/conftest.py")
    assert 'os.environ.setdefault("MASSIVE_API_KEY", "test-massive-api-key")' in source
    assert 'os.environ.setdefault("EARNINGS_API_KEY", "test-earnings-api-key")' in source



def test_integration_client_overrides_readonly_db_dependency() -> None:
    source = _read("tests/integration/conftest.py")
    assert "get_readonly_db" in source
    assert "app.dependency_overrides[get_readonly_db] = override_get_db" in source
