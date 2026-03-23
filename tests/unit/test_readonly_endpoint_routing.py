"""Verify read-heavy endpoints use the read-only DB dependency."""
from __future__ import annotations

from pathlib import Path


def _router_source(path: str) -> str:
    return Path(path).read_text()


def test_backtests_router_uses_readonly_db_for_read_heavy_endpoints() -> None:
    source = _router_source("apps/api/app/routers/backtests.py")
    assert "get_readonly_db" in source
    assert source.count("db: Session = Depends(get_readonly_db)") >= 2


def test_scans_router_uses_readonly_db_for_list_and_recommendations() -> None:
    source = _router_source("apps/api/app/routers/scans.py")
    assert "get_readonly_db" in source
    assert source.count("db: Session = Depends(get_readonly_db)") >= 2


def test_sweeps_router_uses_readonly_db_for_list_and_results() -> None:
    source = _router_source("apps/api/app/routers/sweeps.py")
    assert "get_readonly_db" in source
    assert source.count("db: Session = Depends(get_readonly_db)") >= 2


def test_daily_picks_router_uses_readonly_db_for_reads() -> None:
    source = _router_source("apps/api/app/routers/daily_picks.py")
    assert "get_readonly_db" in source
    assert source.count("db: Session = Depends(get_readonly_db)") >= 2


def test_exports_router_uses_readonly_db_for_list_and_status() -> None:
    source = _router_source("apps/api/app/routers/exports.py")
    assert "get_readonly_db" in source
    assert source.count("db: Session = Depends(get_readonly_db)") >= 2


def test_templates_router_uses_readonly_db_for_list_and_get() -> None:
    source = _router_source("apps/api/app/routers/templates.py")
    assert "get_readonly_db" in source
    assert source.count("db: Session = Depends(get_readonly_db)") >= 2


def test_me_router_bootstraps_user_via_primary_auth_dependency() -> None:
    source = _router_source("apps/api/app/routers/me.py")
    assert "get_readonly_db" in source
    assert "get_current_user" in source
    assert "get_current_user_readonly" not in source
    assert "db: Session = Depends(get_readonly_db)" in source


def test_meta_router_uses_readonly_db() -> None:
    source = _router_source("apps/api/app/routers/meta.py")
    assert "get_readonly_db" in source
    assert "db: Session = Depends(get_readonly_db)" in source


def test_account_export_uses_readonly_db() -> None:
    source = _router_source("apps/api/app/routers/account.py")
    assert "get_readonly_db" in source
    assert "get_current_user_readonly" in source
    assert "db: Session = Depends(get_readonly_db)" in source


def test_read_heavy_routers_use_readonly_auth_dependency() -> None:
    for path in (
        "apps/api/app/routers/catalog.py",
        "apps/api/app/routers/backtests.py",
        "apps/api/app/routers/scans.py",
        "apps/api/app/routers/sweeps.py",
        "apps/api/app/routers/exports.py",
        "apps/api/app/routers/daily_picks.py",
        "apps/api/app/routers/analysis.py",
        "apps/api/app/routers/templates.py",
        "apps/api/app/routers/events.py",
        "apps/api/app/routers/forecasts.py",
    ):
        source = _router_source(path)
        assert "get_current_user_readonly" in source, path


def test_readonly_auth_dependency_does_not_enable_write_fallback() -> None:
    source = _router_source("apps/api/app/dependencies.py")
    assert "allow_write_fallback=False" in source


def test_events_router_uses_readonly_auth_for_sse() -> None:
    source = _router_source("apps/api/app/routers/events.py")
    assert source.count("Depends(get_current_user_readonly)") >= 5
    assert "create_readonly_session" in source


def test_create_endpoints_bootstrap_user_records() -> None:
    expected = {
        "apps/api/app/routers/backtests.py": ["def create_backtest(", "Depends(get_current_user)", "def compare_backtests(", "Depends(get_current_user)"],
        "apps/api/app/routers/analysis.py": ["def create_analysis(", "Depends(get_current_user)"],
        "apps/api/app/routers/scans.py": ["def create_scan(", "Depends(get_current_user)"],
        "apps/api/app/routers/sweeps.py": ["def create_sweep(", "Depends(get_current_user)"],
        "apps/api/app/routers/templates.py": ["def create_template(", "Depends(get_current_user)"],
        "apps/api/app/routers/exports.py": ["def create_export(", "Depends(get_current_user)"],
    }
    for path, snippets in expected.items():
        source = _router_source(path)
        for snippet in snippets:
            assert snippet in source, (path, snippet)


def test_mutating_endpoints_use_primary_auth_dependency() -> None:
    expected = {
        "apps/api/app/routers/account.py": ["def delete_account(", "Depends(get_current_user)"],
        "apps/api/app/routers/analysis.py": ["def create_analysis(", "def delete_analysis(", "Depends(get_current_user)"],
        "apps/api/app/routers/backtests.py": ["def create_backtest(", "def compare_backtests(", "Depends(get_current_user)"],
        "apps/api/app/routers/exports.py": ["def create_export(", "def retry_failed_export(", "Depends(get_current_user)"],
        "apps/api/app/routers/scans.py": ["def create_scan(", "Depends(get_current_user)"],
        "apps/api/app/routers/sweeps.py": ["def create_sweep(", "Depends(get_current_user)"],
        "apps/api/app/routers/templates.py": ["def create_template(", "def update_template(", "Depends(get_current_user)"],
    }
    for path, snippets in expected.items():
        source = _router_source(path)
        for snippet in snippets:
            assert snippet in source, (path, snippet)
