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


def test_me_router_uses_readonly_db() -> None:
    source = _router_source("apps/api/app/routers/me.py")
    assert "get_readonly_db" in source
    assert "get_current_user_readonly" in source
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
        "apps/api/app/routers/backtests.py",
        "apps/api/app/routers/scans.py",
        "apps/api/app/routers/sweeps.py",
        "apps/api/app/routers/exports.py",
        "apps/api/app/routers/daily_picks.py",
        "apps/api/app/routers/analysis.py",
        "apps/api/app/routers/templates.py",
        "apps/api/app/routers/forecasts.py",
    ):
        source = _router_source(path)
        assert "get_current_user_readonly" in source, path


def test_readonly_auth_dependency_does_not_enable_write_fallback() -> None:
    source = _router_source("apps/api/app/dependencies.py")
    assert "allow_write_fallback=False" in source
