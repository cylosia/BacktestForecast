"""Item 67: Test SSE endpoint handlers use asyncio.to_thread for blocking calls.

Verifies that _check_sse_rate and _verify_ownership are called via
asyncio.to_thread in the SSE endpoint handlers rather than blocking the
event loop directly.
"""
from __future__ import annotations

import ast
import inspect
import textwrap

from apps.api.app.routers import events


def _get_source_calls(func) -> list[str]:
    """Return a list of call expression strings from the function source."""
    source = textwrap.dedent(inspect.getsource(func))
    tree = ast.parse(source)
    calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            calls.append(ast.dump(node.func))
    return calls


def test_backtest_events_uses_to_thread_for_rate_and_ownership():
    source = inspect.getsource(events.backtest_events)
    assert "asyncio.to_thread(_check_sse_rate" in source, (
        "backtest_events must wrap _check_sse_rate with asyncio.to_thread"
    )
    assert "asyncio.to_thread(_verify_ownership" in source, (
        "backtest_events must wrap _verify_ownership with asyncio.to_thread"
    )


def test_scan_events_uses_to_thread_for_rate_and_ownership():
    source = inspect.getsource(events.scan_events)
    assert "asyncio.to_thread(_check_sse_rate" in source
    assert "asyncio.to_thread(_verify_ownership" in source


def test_export_events_uses_to_thread_for_rate_and_ownership():
    source = inspect.getsource(events.export_events)
    assert "asyncio.to_thread(_check_sse_rate" in source
    assert "asyncio.to_thread(_verify_ownership" in source


def test_analysis_events_uses_to_thread_for_rate_and_ownership():
    source = inspect.getsource(events.analysis_events)
    assert "asyncio.to_thread(_check_sse_rate" in source
    assert "asyncio.to_thread(_verify_ownership" in source


def test_check_sse_rate_is_sync():
    """_check_sse_rate must be a sync function (suitable for asyncio.to_thread)."""
    assert not inspect.iscoroutinefunction(events._check_sse_rate)


def test_verify_ownership_is_sync():
    """_verify_ownership must be a sync function (suitable for asyncio.to_thread)."""
    assert not inspect.iscoroutinefunction(events._verify_ownership)
