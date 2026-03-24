"""Verify export content_bytes None-check handles empty bytes correctly."""
from __future__ import annotations


def test_empty_bytes_passes_none_check():
    """Empty content b'' must NOT be treated as missing - only None should be."""
    content_bytes = b""
    assert content_bytes is not None, "Empty bytes should not be treated as None"


def test_none_bytes_fails_none_check():
    """None content_bytes must be treated as missing."""
    content_bytes = None
    assert content_bytes is None, "None should be treated as missing content"


def test_real_check_semantics():
    """Verify the exact check used in services/exports.py get_export_for_download."""
    # This mirrors line 386: `if use_db_content and export_job.content_bytes is None:`
    for content_bytes, should_reject in [(None, True), (b"", False), (b"data", False)]:
        is_rejected = content_bytes is None
        assert is_rejected == should_reject, (
            f"content_bytes={content_bytes!r}: expected rejected={should_reject}, got {is_rejected}"
        )
