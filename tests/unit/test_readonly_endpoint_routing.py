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
