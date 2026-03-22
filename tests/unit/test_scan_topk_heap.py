"""Regression tests for bounded in-memory scan recommendation selection."""
from __future__ import annotations

import inspect

from backtestforecast.services.scans import ScanService


def test_scan_service_uses_heap_based_topk_selection() -> None:
    source = inspect.getsource(ScanService._execute_scan)
    assert "candidates_heap" in source
    assert "heapq.heappush" in source
    assert "heapq.heapreplace" in source
    assert "_MAX_CANDIDATES_IN_MEMORY" not in source


def test_scan_service_keeps_small_bounded_buffer() -> None:
    assert isinstance(ScanService._MIN_TOP_CANDIDATE_BUFFER, int)
    assert ScanService._MIN_TOP_CANDIDATE_BUFFER >= 10
    assert ScanService._MIN_TOP_CANDIDATE_BUFFER <= 200

