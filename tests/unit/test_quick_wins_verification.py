from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (ROOT / relpath).read_text(encoding="utf-8")


def test_create_flows_keep_job_and_dispatch_state_in_service_transaction() -> None:
    source = _read("tests/unit/test_create_dispatch_regressions.py")
    for expected in (
        "test_backtest_create_and_dispatch_preserves_pending_outbox_on_send_failure",
        "test_scan_create_and_dispatch_preserves_pending_outbox_on_send_failure",
        "test_sweep_create_and_dispatch_preserves_pending_outbox_on_send_failure",
        "test_analysis_create_and_dispatch_preserves_pending_outbox_on_send_failure",
        "test_export_create_and_dispatch_preserves_pending_outbox_on_send_failure",
    ):
        assert expected in source
    assert "_assert_pending_outbox" in source
    assert "OutboxMessage" in source


def test_stale_queued_idempotency_recovery_is_covered_for_all_async_flows() -> None:
    source = _read("tests/unit/test_create_dispatch_regressions.py")
    for expected in (
        "test_backtest_idempotency_reuse_redispatches_stale_queued_job",
        "test_scan_idempotency_reuse_redispatches_stale_queued_job",
        "test_sweep_idempotency_reuse_redispatches_stale_queued_job",
        "test_analysis_idempotency_reuse_redispatches_stale_queued_job",
        "test_export_idempotency_reuse_redispatches_stale_queued_job",
    ):
        assert expected in source
    assert "_assert_stale_job_redispatched" in source


def test_daily_picks_history_cursor_pagination_reaches_web_layer() -> None:
    page_source = _read("apps/web/app/app/daily-picks/page.tsx")
    api_source = _read("apps/web/lib/api/server.ts")

    assert "searchParams: Promise<{ next_cursor?: string; cursor?: string }>" in page_source
    assert "const cursor = params.next_cursor?.trim() || params.cursor?.trim() || undefined;" in page_source
    assert "await getDailyPicksHistory(HISTORY_PAGE_SIZE, cursor)" in page_source
    assert 'cursorParamName="next_cursor"' in page_source
    assert 'buildCursorPaginatedPath("/v1/daily-picks/history", limit, 30, cursor)' in api_source


def test_template_contracts_fail_hard_without_any_bypasses() -> None:
    page_source = _read("apps/web/app/app/templates/page.tsx")
    contract_source = _read("apps/web/lib/templates/contracts.ts")

    assert "as any" not in page_source
    assert "as any" not in contract_source
    assert ": any" not in contract_source
    assert "isValidTemplateConfig(data.config)" in contract_source
    assert "return data as TemplateListResponse;" in contract_source


def test_structured_logging_and_readonly_routing_quick_wins_have_guardrails() -> None:
    config_source = _read("src/backtestforecast/config.py")
    readonly_tests = _read("tests/unit/test_readonly_endpoint_routing.py")

    assert "config.massive_api_key_missing" in config_source
    assert "logger.warning(" in config_source
    assert "warnings.warn(" not in config_source
    assert "test_backtests_router_uses_readonly_db_for_read_heavy_endpoints" in readonly_tests
    assert "test_scans_router_uses_readonly_db_for_list_and_recommendations" in readonly_tests
    assert "test_sweeps_router_uses_readonly_db_for_list_and_results" in readonly_tests
    assert "test_daily_picks_router_uses_readonly_db_for_reads" in readonly_tests


def test_web_request_budget_and_schedule_copy_quick_wins_are_guarded() -> None:
    budget_source = _read("apps/web/__tests__/server-component-budget.test.ts")
    page_source = _read("apps/web/app/app/daily-picks/page.tsx")

    assert "memoizes current-user loading behind a cached token-keyed loader" in budget_source
    assert "collapse duplicate /v1/me reads" in budget_source
    assert "avoids an extra /v1/meta round trip on the daily-picks page" in budget_source
    assert "getDailyPicksScheduleLabel()" in budget_source
    assert "const scheduleLabel = getDailyPicksScheduleLabel();" in page_source


def test_shared_version_constant_and_live_sse_path_have_explicit_coverage() -> None:
    version_source = _read("src/backtestforecast/version.py")
    metrics_source = _read("src/backtestforecast/observability/metrics.py")
    worker_source = _read("apps/worker/app/celery_app.py")
    events_source = _read("src/backtestforecast/events.py")
    e2e_source = _read("apps/web/e2e/daily-picks.spec.ts")

    assert 'PROMETHEUS_TEXT_FORMAT_VERSION = "0.0.4"' in version_source
    assert "PROMETHEUS_TEXT_FORMAT_VERSION" in metrics_source
    assert "PROMETHEUS_TEXT_FORMAT_VERSION" in worker_source
    assert "pollers/SSE subscribers can update job progress in near real time" in events_source
    assert "SSE proxy forwards authenticated requests to the API backend" in e2e_source
    assert 'fetch("/api/events/backtests/00000000-0000-0000-0000-000000000000"' in e2e_source
