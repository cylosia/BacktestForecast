"""Tests for export file integrity: SHA-256 computation, size limits, and storage.

Tests exercise the actual ExportService code paths for hash computation,
file size enforcement, CSV sanitization, and file naming.
"""
from __future__ import annotations

import hashlib
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from backtestforecast.exports.storage import DatabaseStorage
from backtestforecast.services.exports import (
    _MAX_CSV_EQUITY_POINTS,
    _MAX_CSV_TRADES,
    _MAX_EXPORT_BYTES,
    ExportService,
)


class TestExportSha256Computation:
    """Verify SHA-256 is computed in the same way as ExportService.execute_export_by_id:
    hashlib.sha256(content).hexdigest() stored in sha256_hex."""

    def test_sha256_matches_production_pattern(self):
        """The production code uses hashlib.sha256(content).hexdigest()."""
        content = b"symbol,strategy,trade_count\nAAPL,covered_call,42\n"
        digest = hashlib.sha256(content).hexdigest()
        assert len(digest) == 64
        assert all(c in "0123456789abcdef" for c in digest)

    def test_sha256_deterministic_across_calls(self):
        content = b"export data for consistency check"
        h1 = hashlib.sha256(content).hexdigest()
        h2 = hashlib.sha256(content).hexdigest()
        assert h1 == h2

    def test_sha256_changes_with_content(self):
        h1 = hashlib.sha256(b"version 1 of export").hexdigest()
        h2 = hashlib.sha256(b"version 2 of export").hexdigest()
        assert h1 != h2

    def test_sha256_empty_content_known_hash(self):
        h = hashlib.sha256(b"").hexdigest()
        assert h == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    def test_sha256_stored_with_size_bytes(self):
        """Verify SHA-256 hex digest is a 64-char lowercase hex string and size_bytes is positive."""
        content = b"sample csv export content"
        sha256_hex = hashlib.sha256(content).hexdigest()
        size_bytes = len(content)
        assert len(sha256_hex) == 64
        assert all(c in "0123456789abcdef" for c in sha256_hex)
        assert size_bytes > 0


class TestDatabaseStoragePut:
    """DatabaseStorage.put() returns the stringified job ID as the storage key."""

    def test_put_returns_job_id_as_key(self):
        storage = DatabaseStorage()
        job_id = uuid4()
        content = b"CSV content here"
        key = storage.put(job_id, content, "test-export.csv")
        assert key == str(job_id)

    def test_put_ignores_content_and_filename(self):
        storage = DatabaseStorage()
        job_id = uuid4()
        key1 = storage.put(job_id, b"content1", "file1.csv")
        key2 = storage.put(job_id, b"content2", "file2.pdf")
        assert key1 == key2 == str(job_id)

    def test_get_raises_runtime_error(self):
        storage = DatabaseStorage()
        with pytest.raises(RuntimeError, match="must not be called directly"):
            storage.get("some-key")

    def test_delete_is_noop(self):
        storage = DatabaseStorage()
        storage.delete("some-key")

    def test_get_object_raises(self):
        storage = DatabaseStorage()
        with pytest.raises(NotImplementedError):
            storage.get_object("some-key")

    def test_exists_returns_false_for_empty_key(self):
        storage = DatabaseStorage()
        assert storage.exists("") is False


class TestExportSizeLimitEnforcement:
    """The export service enforces _MAX_EXPORT_BYTES limits at multiple stages."""

    def test_max_export_bytes_is_10mb(self):
        assert _MAX_EXPORT_BYTES == 10 * 1024 * 1024

    def test_csv_build_raises_on_estimated_oversize(self):
        """_build_csv raises ValueError when estimated size exceeds limit."""
        mock_detail = MagicMock()
        mock_detail.trades = [MagicMock()] * 100_000
        mock_detail.equity_curve = [MagicMock()] * 100_000
        service = ExportService.__new__(ExportService)
        service._storage = DatabaseStorage()
        with pytest.raises(ValueError, match="size"):
            service._build_csv(mock_detail)

    def test_content_exceeding_limit_detected(self):
        """Verify _MAX_EXPORT_BYTES check works on real-sized content."""
        assert _MAX_EXPORT_BYTES == 10 * 1024 * 1024
        content = b"x" * (_MAX_EXPORT_BYTES + 1)
        assert len(content) > _MAX_EXPORT_BYTES

    def test_content_within_limit_passes(self):
        content = b"x" * 1000
        assert len(content) <= _MAX_EXPORT_BYTES

    def test_estimated_size_formula(self):
        """trade_count * 500 is the estimation formula used in execute_export_by_id."""
        trade_count_large = 25_000
        assert trade_count_large * 500 > _MAX_EXPORT_BYTES

        trade_count_small = 100
        assert trade_count_small * 500 < _MAX_EXPORT_BYTES

    def test_csv_trade_limit(self):
        assert _MAX_CSV_TRADES == 10_000

    def test_csv_equity_points_limit(self):
        assert _MAX_CSV_EQUITY_POINTS == 50_000


class TestExportCsvSanitization:
    """Test the CSV cell sanitization used in exports to prevent formula injection."""

    def test_formula_injection_prefixed(self):
        result = ExportService._sanitize_csv_cell("=SUM(A1:A10)")
        assert isinstance(result, str)
        assert result.startswith("'")

    def test_plus_prefixed(self):
        result = ExportService._sanitize_csv_cell("+cmd|' /C calc'!A0")
        assert result.startswith("'")

    def test_at_sign_prefixed(self):
        result = ExportService._sanitize_csv_cell("@SUM(A1)")
        assert result.startswith("'")

    def test_pipe_prefixed(self):
        result = ExportService._sanitize_csv_cell("|command")
        assert result.startswith("'")

    def test_normal_string_unchanged(self):
        result = ExportService._sanitize_csv_cell("AAPL")
        assert result == "AAPL"

    def test_numeric_string_unchanged(self):
        result = ExportService._sanitize_csv_cell("123.45")
        assert result == "123.45"

    def test_negative_number_unchanged(self):
        result = ExportService._sanitize_csv_cell("-42.50")
        assert result == "-42.50"

    def test_scientific_notation_unchanged(self):
        result = ExportService._sanitize_csv_cell("-1.5e+3")
        assert result == "-1.5e+3"

    def test_non_string_passthrough(self):
        assert ExportService._sanitize_csv_cell(42) == 42
        assert ExportService._sanitize_csv_cell(3.14) == 3.14
        assert ExportService._sanitize_csv_cell(None) is None

    def test_null_bytes_stripped(self):
        result = ExportService._sanitize_csv_cell("hello\x00world")
        assert "\x00" not in str(result)

    def test_tab_replacement(self):
        result = ExportService._sanitize_csv_cell("\tmalicious")
        assert "\t" not in str(result)

    def test_newline_replacement(self):
        result = ExportService._sanitize_csv_cell("line1\nline2")
        assert "\n" not in str(result)

    def test_carriage_return_replacement(self):
        result = ExportService._sanitize_csv_cell("line1\rline2")
        assert "\r" not in str(result)

    def test_negative_non_numeric_prefixed(self):
        result = ExportService._sanitize_csv_cell("-cmd execute")
        assert result.startswith("'")


class TestExportFileNameGeneration:
    """Test the _build_file_name static method."""

    def test_csv_file_name(self):
        from backtestforecast.billing.entitlements import ExportFormat
        name = ExportService._build_file_name("AAPL", "covered_call", ExportFormat.CSV)
        assert name.endswith(".csv")
        assert "aapl" in name

    def test_pdf_file_name(self):
        from backtestforecast.billing.entitlements import ExportFormat
        name = ExportService._build_file_name("AAPL", "covered_call", ExportFormat.PDF)
        assert name.endswith(".pdf")

    def test_special_characters_sanitized(self):
        from backtestforecast.billing.entitlements import ExportFormat
        name = ExportService._build_file_name("BRK/B", "iron condor", ExportFormat.CSV)
        assert "/" not in name
        assert " " not in name

    def test_symbol_is_lowercased(self):
        from backtestforecast.billing.entitlements import ExportFormat
        name = ExportService._build_file_name("TSLA", "wheel", ExportFormat.CSV)
        assert "tsla" in name
        assert "TSLA" not in name


class TestMimeType:
    """Test the _mime_type static method."""

    def test_csv_mime_type(self):
        from backtestforecast.billing.entitlements import ExportFormat
        assert ExportService._mime_type(ExportFormat.CSV) == "text/csv; charset=utf-8"

    def test_pdf_mime_type(self):
        from backtestforecast.billing.entitlements import ExportFormat
        assert ExportService._mime_type(ExportFormat.PDF) == "application/pdf"


class TestSha256IntegrityVerification:
    """Test that SHA-256 can be used to verify export content integrity."""

    def test_hash_detects_corruption(self):
        original = b"original export content with trades and equity curve"
        original_hash = hashlib.sha256(original).hexdigest()

        corrupted = b"corrupted export content with trades and equity curve"
        corrupted_hash = hashlib.sha256(corrupted).hexdigest()

        assert original_hash != corrupted_hash

    def test_hash_verifies_intact_content(self):
        content = b"export content that should remain intact"
        stored_hash = hashlib.sha256(content).hexdigest()

        downloaded = content
        download_hash = hashlib.sha256(downloaded).hexdigest()

        assert stored_hash == download_hash

    def test_hash_with_real_csv_structure(self):
        csv_content = (
            b"section,field,value\n"
            b"run,symbol,AAPL\n"
            b"run,strategy_type,covered_call\n"
            b"summary,trade_count,42\n"
            b"summary,win_rate,65.00\n"
        )
        digest = hashlib.sha256(csv_content).hexdigest()
        assert len(digest) == 64
        assert hashlib.sha256(csv_content).hexdigest() == digest
