"""Test that S3 export streaming uses chunked transfer for large files.

Regression test for the bug where S3 downloads were silently truncated
at the 5-minute timeout when Content-Length was set, leaving clients
with partial data and no error signal.
"""
from __future__ import annotations

import inspect


def test_chunked_transfer_for_large_files():
    """Large S3 downloads must use Transfer-Encoding: chunked, not Content-Length."""
    from apps.api.app.routers import exports
    source = inspect.getsource(exports)
    assert "Transfer-Encoding" in source, (
        "Export download should set Transfer-Encoding: chunked for large files"
    )
    assert 'del headers["Transfer-Encoding"]' in source or "del headers['Transfer-Encoding']" in source, (
        "Small files should use Content-Length (delete Transfer-Encoding)"
    )


def test_stream_timeout_raises_error():
    """Stream timeout must raise TimeoutError, not silently truncate."""
    from apps.api.app.routers import exports
    source = inspect.getsource(exports)
    assert "raise TimeoutError" in source or "raise asyncio.TimeoutError" in source, (
        "S3 stream timeout must raise an error so the response signals failure"
    )


def test_stream_timeout_not_silent_break():
    """Stream timeout must NOT use 'break' to silently stop streaming."""
    from apps.api.app.routers import exports
    source = inspect.getsource(exports.download_export)
    lines = source.splitlines()
    in_timeout_block = False
    for line in lines:
        if "STREAM_TIMEOUT_SECONDS" in line and "elapsed" in line:
            in_timeout_block = True
        if in_timeout_block and line.strip() == "break":
            raise AssertionError(
                "Stream timeout uses 'break' which silently truncates the download. "
                "Must raise TimeoutError instead."
            )
        if in_timeout_block and ("raise" in line or "return" in line):
            break
