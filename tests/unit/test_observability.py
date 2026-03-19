"""Verify observability metrics and logging configuration."""
from __future__ import annotations

import pytest

from backtestforecast.observability import metrics
from backtestforecast.observability.logging import _sanitize_sensitive_keys
from backtestforecast.observability.metrics import _normalize_path


def test_all_metrics_have_unique_names():
    """Ensure no two metrics share the same Prometheus name."""
    names: dict[str, str] = {}
    for attr_name in dir(metrics):
        if attr_name.startswith("_"):
            continue
        obj = getattr(metrics, attr_name)
        name = getattr(obj, "_name", None)
        if name is None:
            continue
        assert name not in names, (
            f"Duplicate metric name '{name}': "
            f"used by both {names[name]} and {attr_name}"
        )
        names[name] = attr_name


def test_http_path_normalization_uuid():
    """Verify UUID path segments are collapsed to {id}."""
    result = _normalize_path("/v1/backtests/123e4567-e89b-12d3-a456-426614174000")
    assert result == "/v1/backtests/{id}"


def test_http_path_normalization_integer():
    """Verify integer path segments are collapsed to {id}."""
    result = _normalize_path("/v1/scans/42/recommendations")
    assert result == "/v1/scans/{id}/recommendations"


def test_http_path_normalization_unknown():
    """Unknown paths are collapsed to /unknown to prevent cardinality explosion."""
    result = _normalize_path("/some/random/path")
    assert result == "/unknown"


def test_sensitive_key_redaction():
    """Verify that passwords and tokens are redacted in structured logs."""
    event_dict = {
        "password": "secret123",
        "api_key": "sk_live_abc",
        "token": "jwt_token_value",
        "name": "test-user",
        "event": "test.event",
    }
    sanitized = _sanitize_sensitive_keys(None, None, dict(event_dict))
    assert sanitized["password"] == "[REDACTED]"
    assert sanitized["api_key"] == "[REDACTED]"
    assert sanitized["token"] == "[REDACTED]"
    assert sanitized["name"] == "test-user"
    assert sanitized["event"] == "test.event"


def test_sensitive_key_redaction_nested():
    """Verify redaction works on nested dicts."""
    event_dict = {
        "request": {"authorization": "Bearer xyz", "path": "/v1/me"},
        "event": "test.nested",
    }
    sanitized = _sanitize_sensitive_keys(None, None, dict(event_dict))
    assert sanitized["request"]["authorization"] == "[REDACTED]"
    assert sanitized["request"]["path"] == "/v1/me"


def test_sensitive_key_redaction_preserves_non_sensitive():
    """Non-sensitive keys should pass through unchanged."""
    event_dict = {"user_id": "u123", "status": 200, "event": "request.completed"}
    sanitized = _sanitize_sensitive_keys(None, None, dict(event_dict))
    assert sanitized == event_dict
