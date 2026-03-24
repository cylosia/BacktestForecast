"""Test 66: ThreadPoolExecutor timeout cancellation.

Verifies that when as_completed times out, remaining futures are properly
cancelled via cancel_futures=True in the shutdown call (mirrors the pattern
used in deep_analysis._build_landscape).
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeoutError

import pytest


def _slow_task(duration: float) -> str:
    time.sleep(duration)
    return "completed"


def _fast_task() -> str:
    return "fast"


class TestThreadPoolCancelFutures:
    def test_cancel_futures_on_shutdown(self):
        """pool.shutdown(cancel_futures=True) cancels pending futures."""
        import threading

        hold = threading.Event()
        pool = ThreadPoolExecutor(max_workers=1)
        try:
            pool.submit(lambda: (hold.wait(timeout=5.0), "done")[1])
            pending = pool.submit(_slow_task, 10.0)
            pool.shutdown(wait=False, cancel_futures=True)
        finally:
            hold.set()

        assert pending.cancelled() or pending.done()

    def test_completed_futures_unaffected_by_cancel(self):
        """Futures that finished before shutdown retain their results."""
        pool = ThreadPoolExecutor(max_workers=2)
        results: list[str] = []
        try:
            fast = pool.submit(_fast_task)
            slow = pool.submit(_slow_task, 10.0)
            try:
                for f in as_completed([fast, slow], timeout=1.0):
                    try:
                        results.append(f.result(timeout=1.0))
                    except Exception:
                        continue
            except FuturesTimeoutError:
                pass
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

        assert "fast" in results, "Fast task result should be collected before timeout"

    def test_timeout_triggers_timeout_error(self):
        """as_completed raises TimeoutError when futures exceed timeout."""
        pool = ThreadPoolExecutor(max_workers=1)
        try:
            future = pool.submit(_slow_task, 10.0)
            with pytest.raises(FuturesTimeoutError):
                for f in as_completed([future], timeout=0.1):
                    f.result(timeout=0.1)
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    def test_landscape_pattern_collects_partial_results(self):
        """Mirrors _build_landscape: collect what finishes, skip the rest."""
        pool = ThreadPoolExecutor(max_workers=2)
        cells: list[str] = []
        try:
            futures = {
                pool.submit(_fast_task): "fast_strat",
                pool.submit(_slow_task, 10.0): "slow_strat",
            }
            try:
                for future in as_completed(futures, timeout=1.0):
                    try:
                        result = future.result(timeout=1.0)
                        cells.append(result)
                    except Exception:
                        continue
            except FuturesTimeoutError:
                pass
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

        assert "fast" in cells
        assert "completed" not in cells
