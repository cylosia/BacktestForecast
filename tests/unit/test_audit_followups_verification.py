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


def test_dispatch_helper_has_tracing_span_and_outbox_correlation_logging() -> None:
    source = _read("apps/api/app/dispatch.py")
    assert 'start_as_current_span' in source
    assert '"dispatch.outbox_written"' in source
    assert 'outbox_id=' in source
    assert 'job_id=' in source


def test_routers_delegate_dispatch_to_services_only() -> None:
    for relpath in (
        "apps/api/app/routers/backtests.py",
        "apps/api/app/routers/scans.py",
        "apps/api/app/routers/sweeps.py",
        "apps/api/app/routers/exports.py",
        "apps/api/app/routers/analysis.py",
    ):
        source = _read(relpath)
        assert "dispatch_celery_task" not in source
        assert "create_and_dispatch" in source


def test_reconciliation_and_support_tooling_exist_for_stranded_jobs() -> None:
    worker_source = _read("apps/worker/app/tasks.py")
    assert "maintenance.reconcile_stranded_jobs" in worker_source
    assert "repair_stranded_jobs(" in worker_source

    celery_source = _read("apps/worker/app/celery_app.py")
    assert '"maintenance.reconcile_stranded_jobs"' in celery_source
    assert '"reconcile-stranded-jobs"' in celery_source

    script_source = _read("scripts/repair_stranded_jobs.py")
    assert "--action" in script_source
    assert "repair_stranded_jobs(" in script_source
