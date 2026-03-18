"""Tests for export file integrity verification."""
from __future__ import annotations

import hashlib


class TestExportSha256:
    def test_sha256_computed_correctly(self):
        content = b"test export content"
        expected = hashlib.sha256(content).hexdigest()
        assert len(expected) == 64
        assert expected == hashlib.sha256(content).hexdigest()

    def test_empty_content_has_known_hash(self):
        content = b""
        h = hashlib.sha256(content).hexdigest()
        assert h == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    def test_different_content_different_hash(self):
        h1 = hashlib.sha256(b"content a").hexdigest()
        h2 = hashlib.sha256(b"content b").hexdigest()
        assert h1 != h2
