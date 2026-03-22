from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (ROOT / relpath).read_text(encoding="utf-8")


def test_backtests_page_wires_paginated_history_contract() -> None:
    source = _read("apps/web/app/app/backtests/page.tsx")
    assert 'searchParams: Promise<{ offset?: string; limit?: string; cursor?: string }>' in source
    assert "const cursor = params.cursor?.trim() || undefined;" in source
    assert "getBacktestHistory(limit, offset, cursor)" in source
    assert "PaginationControls" in source
    assert 'basePath="/app/backtests"' in source
    assert "nextCursor={history.next_cursor}" in source


def test_scanner_page_wires_paginated_history_contract() -> None:
    source = _read("apps/web/app/app/scanner/page.tsx")
    assert 'searchParams: Promise<{ offset?: string; cursor?: string }>' in source
    assert "const cursor = params.cursor?.trim() || undefined;" in source
    assert "getScannerJobs(PAGE_SIZE, offset, cursor)" in source
    assert "PaginationControls" in source
    assert 'basePath="/app/scanner"' in source
    assert "nextCursor={jobs.next_cursor}" in source


def test_sweeps_page_wires_paginated_history_contract() -> None:
    source = _read("apps/web/app/app/sweeps/page.tsx")
    assert 'searchParams: Promise<{ offset?: string; cursor?: string }>' in source
    assert "const cursor = params.cursor?.trim() || undefined;" in source
    assert "getSweepJobs(PAGE_SIZE, offset, cursor)" in source
    assert "PaginationControls" in source
    assert 'basePath="/app/sweeps"' in source
    assert "nextCursor={jobs.next_cursor}" in source


def test_analysis_page_wires_paginated_history_contract() -> None:
    source = _read("apps/web/app/app/analysis/page.tsx")
    assert 'searchParams: Promise<{ offset?: string; cursor?: string }>' in source
    assert "const cursor = params.cursor?.trim() || undefined;" in source
    assert "getAnalysisHistory(PAGE_SIZE, offset, cursor)" in source
    assert "PaginationControls" in source
    assert 'basePath="/app/analysis"' in source
    assert "nextCursor={history.next_cursor}" in source


def test_daily_picks_page_wires_cursor_paginated_history_contract() -> None:
    source = _read("apps/web/app/app/daily-picks/page.tsx")
    assert 'searchParams: Promise<{ next_cursor?: string; cursor?: string }>' in source
    assert "const cursor = params.next_cursor?.trim() || params.cursor?.trim() || undefined;" in source
    assert "getDailyPicksHistory(HISTORY_PAGE_SIZE, cursor)" in source
    assert "PaginationControls" in source
    assert 'basePath="/app/daily-picks"' in source
    assert 'cursorParamName="next_cursor"' in source
    assert "nextCursor={history.next_cursor}" in source
