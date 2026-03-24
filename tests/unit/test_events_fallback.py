"""Tests for event publishing constants and logic.

Avoids importing backtestforecast.events directly at module level to prevent
Prometheus metric re-registration conflicts when running all unit tests.
"""
from __future__ import annotations


class TestEventConstants:
    def test_reserved_keys_protected(self):
        from backtestforecast.events import _RESERVED_PAYLOAD_KEYS
        assert "v" in _RESERVED_PAYLOAD_KEYS
        assert "status" in _RESERVED_PAYLOAD_KEYS
        assert "job_id" in _RESERVED_PAYLOAD_KEYS

    def test_valid_target_statuses(self):
        from backtestforecast.events import _VALID_TARGET_STATUSES
        assert "succeeded" in _VALID_TARGET_STATUSES
        assert "failed" in _VALID_TARGET_STATUSES
        assert "cancelled" in _VALID_TARGET_STATUSES

    def test_export_valid_target_statuses_includes_expired(self):
        from backtestforecast.events import _EXPORT_VALID_TARGET_STATUSES, _VALID_TARGET_STATUSES
        assert "expired" in _EXPORT_VALID_TARGET_STATUSES
        assert _VALID_TARGET_STATUSES.issubset(_EXPORT_VALID_TARGET_STATUSES)
