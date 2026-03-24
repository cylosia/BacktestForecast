from __future__ import annotations

from pathlib import Path

from backtestforecast.schemas.analysis import CreateAnalysisRequest
from backtestforecast.schemas.backtests import CreateBacktestRunRequest
from backtestforecast.schemas.exports import CreateExportRequest
from backtestforecast.schemas.scans import CreateScannerJobRequest
from backtestforecast.schemas.sweeps import CreateSweepRequest
from backtestforecast.utils.schedules import format_utc_schedule_label

ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (ROOT / relpath).read_text(encoding="utf-8")


def test_no_any_guard_has_contract_test_coverage() -> None:
    source = _read("scripts/check_no_any_in_app_pages.py")
    assert "API_IMPORT_MARKERS" in source
    assert "disallowed `any` in API-backed app page" in source


def test_health_and_admin_surface_queue_diagnostics() -> None:
    health_source = _read("apps/api/app/routers/health.py")
    main_source = _read("apps/api/app/main.py")
    assert '"queue_diagnostics"' in health_source
    assert '"queue_diagnostics"' in main_source
    assert "get_queue_diagnostics" in health_source


def test_idempotency_fields_have_explicit_retry_safe_descriptions() -> None:
    for field in (
        CreateBacktestRunRequest.model_fields["idempotency_key"],
        CreateScannerJobRequest.model_fields["idempotency_key"],
        CreateExportRequest.model_fields["idempotency_key"],
        CreateSweepRequest.model_fields["idempotency_key"],
        CreateAnalysisRequest.model_fields["idempotency_key"],
    ):
        assert field.description is not None
        assert "retry-safe" in field.description


def test_python_schedule_formatter_matches_expected_copy() -> None:
    assert format_utc_schedule_label(6, 0) == "6:00 AM UTC"
    assert format_utc_schedule_label(18, 5) == "6:05 PM UTC"


def test_server_component_layout_no_longer_logs_console_errors() -> None:
    source = _read("apps/web/app/app/layout.tsx")
    assert "console.error" not in source
    assert "try {" in source
    assert "await getCurrentUser();" in source
    assert "} catch {" in source


def test_dispatch_started_at_added_to_all_async_job_models() -> None:
    source = _read("src/backtestforecast/models.py")
    for model_name in ("BacktestRun", "ScannerJob", "ExportJob", "SymbolAnalysis", "SweepJob"):
        assert model_name in source
    assert source.count("dispatch_started_at: Mapped[datetime | None]") >= 5


def test_sse_resources_and_task_route_names_stay_consistent() -> None:
    tasks_source = _read("src/backtestforecast/services/dispatch_recovery.py")
    events_source = _read("apps/api/app/routers/events.py")
    expected = {
        "backtests.run": "/backtests/",
        "scans.run_job": "/scans/",
        "sweeps.run": "/sweeps/",
        "exports.generate": "/exports/",
        "analysis.deep_symbol": "/analyses/",
    }
    for task_name, resource_path in expected.items():
        assert task_name in tasks_source
        assert resource_path in events_source


def test_repo_get_routes_keep_readonly_session_audit_guardrails() -> None:
    source = _read("tests/unit/test_readonly_endpoint_routing.py")
    assert "test_read_heavy_routers_use_readonly_auth_dependency" in source
    assert "test_backtests_router_uses_readonly_db_for_read_heavy_endpoints" in source
