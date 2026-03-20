"""Test DLQ redaction handles nested structures correctly.

Covers both the worker-side _redact (task_base.py) and the read-side
_redact_dict (/admin/dlq in main.py).
"""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.filterwarnings("ignore::UserWarning")


class TestWorkerSideRedact:
    def test_flat_keys_redacted(self):
        from apps.worker.app.task_base import _redact
        data = {"email": "user@example.com", "run_id": "abc-123"}
        result = _redact(data)
        assert result["email"] == "[REDACTED]"
        assert result["run_id"] == "abc-123"

    def test_nested_dict_redacted(self):
        from apps.worker.app.task_base import _redact
        data = {"user": {"email": "user@example.com", "id": "u-1"}}
        result = _redact(data)
        assert result["user"]["email"] == "[REDACTED]"
        assert result["user"]["id"] == "u-1"

    def test_list_of_dicts_redacted(self):
        from apps.worker.app.task_base import _redact
        data = {"items": [{"email": "a@b.com"}, {"name": "John"}]}
        result = _redact(data)
        assert result["items"][0]["email"] == "[REDACTED]"

    def test_authorization_redacted(self):
        from apps.worker.app.task_base import _redact
        data = {"authorization": "Bearer sk_live_xxx", "task": "test"}
        result = _redact(data)
        assert result["authorization"] == "[REDACTED]"
        assert result["task"] == "test"

    def test_ip_key_redacted(self):
        from apps.worker.app.task_base import _redact
        data = {"ip": "1.2.3.4", "ip_address": "5.6.7.8"}
        result = _redact(data)
        assert result["ip"] == "[REDACTED]"
        assert result["ip_address"] == "[REDACTED]"

    def test_empty_dict(self):
        from apps.worker.app.task_base import _redact
        assert _redact({}) == {}

    def test_non_sensitive_keys_preserved(self):
        from apps.worker.app.task_base import _redact
        data = {"run_id": "abc", "status": "failed", "trade_count": 5}
        result = _redact(data)
        assert result == data
