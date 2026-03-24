"""Tests for _normalize_origin function - Contract Mismatch #3 fix.

Verifies that origin normalization handles:
- Trailing slashes
- Case differences
- Default port stripping (:443 for https, :80 for http)
"""
from __future__ import annotations

import pytest

from apps.api.app.dependencies import _normalize_origin


@pytest.mark.parametrize("raw,expected", [
    ("https://example.com", "https://example.com"),
    ("HTTPS://EXAMPLE.COM", "https://example.com"),
    ("https://example.com/", "https://example.com"),
    ("https://example.com///", "https://example.com"),
    ("  https://example.com  ", "https://example.com"),
    ("https://example.com:443", "https://example.com"),
    ("https://example.com:8443", "https://example.com:8443"),
    ("http://localhost:3000", "http://localhost:3000"),
    ("http://localhost:80", "http://localhost"),
    ("http://localhost:3000/", "http://localhost:3000"),
])
def test_normalize_origin(raw: str, expected: str) -> None:
    assert _normalize_origin(raw) == expected
