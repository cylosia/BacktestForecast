"""Item 71: Test pipeline future timeout behavior.

Verifies that when a pipeline future exceeds its timeout, the pipeline
handles it gracefully (continues processing other symbols, does not crash).
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeoutError

import pytest


def _slow_task(duration: float) -> str:
    time.sleep(duration)
    return "done"


class TestPipelineFutureTimeout:
    def test_as_completed_raises_timeout_error(self):
        """When a future exceeds the timeout passed to as_completed,
        iterating over the results should raise TimeoutError."""
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_slow_task, 5.0)
            with pytest.raises(FuturesTimeoutError):
                for f in as_completed([future], timeout=0.1):
                    f.result(timeout=0.1)

    def test_future_result_timeout_raises(self):
        """Calling future.result(timeout=...) on a long-running task
        raises TimeoutError."""
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_slow_task, 5.0)
            with pytest.raises(FuturesTimeoutError):
                future.result(timeout=0.1)

    def test_pipeline_stage_pattern_skips_timed_out_futures(self):
        """Simulate the pipeline's pattern: iterate as_completed with a
        timeout and continue on TimeoutError so that successful futures
        are still collected."""

        def fast_task() -> str:
            return "fast"

        results: list[str] = []
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {
                pool.submit(fast_task): "fast_sym",
                pool.submit(_slow_task, 10.0): "slow_sym",
            }
            try:
                for future in as_completed(futures, timeout=1.0):
                    try:
                        result = future.result(timeout=1.0)
                        results.append(result)
                    except Exception:
                        continue
            except FuturesTimeoutError:
                pass

        assert "fast" in results
        assert "done" not in results
