from __future__ import annotations

from pathlib import Path

from apps.api.app.routers.analysis import router as analysis_router
from apps.api.app.routers.backtests import router as backtests_router
from apps.api.app.routers.exports import router as exports_router
from apps.api.app.routers.scans import router as scans_router
from apps.api.app.routers.sweeps import router as sweeps_router


def _route_paths(router, method: str) -> set[str]:
    target = method.upper()
    return {
        route.path
        for route in router.routes
        if getattr(route, "methods", None) and target in route.methods
    }


def test_cancel_routes_are_exposed_for_user_visible_jobs() -> None:
    assert "/backtests/{run_id}/cancel" in _route_paths(backtests_router, "POST")
    assert "/exports/{export_job_id}/cancel" in _route_paths(exports_router, "POST")
    assert "/scans/{job_id}/cancel" in _route_paths(scans_router, "POST")
    assert "/sweeps/{job_id}/cancel" in _route_paths(sweeps_router, "POST")
    assert "/analysis/{analysis_id}/cancel" in _route_paths(analysis_router, "POST")


def test_scan_delete_message_no_longer_claims_missing_cancel_flow() -> None:
    for path in (
        "src/backtestforecast/services/backtests.py",
        "src/backtestforecast/services/exports.py",
        "src/backtestforecast/services/scans.py",
        "src/backtestforecast/services/sweeps.py",
        "src/backtestforecast/pipeline/deep_analysis.py",
        "apps/api/app/routers/analysis.py",
    ):
        source = Path(path).read_text(encoding="utf-8")
        assert "Cancel it first." not in source, path
        assert "cancel it first" not in source.lower(), path


def test_cancel_routes_use_primary_auth_dependency() -> None:
    expectations = {
        "apps/api/app/routers/backtests.py": "def cancel_backtest(",
        "apps/api/app/routers/exports.py": "def cancel_export(",
        "apps/api/app/routers/scans.py": "def cancel_scan(",
        "apps/api/app/routers/sweeps.py": "def cancel_sweep(",
        "apps/api/app/routers/analysis.py": "def cancel_analysis(",
    }
    for path, marker in expectations.items():
        source = Path(path).read_text(encoding="utf-8")
        start = source.find(marker)
        assert start >= 0, path
        end = source.find("@router", start + 1)
        block = source[start:end] if end >= 0 else source[start:]
        assert "Depends(get_current_user)" in block, path
        assert "Depends(get_current_user_readonly)" not in block, path
