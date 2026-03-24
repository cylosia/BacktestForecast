"""Tests for custom SQLAlchemy types."""
from __future__ import annotations

import uuid

from backtestforecast.db.types import GUID


class TestGUID:
    def test_cache_ok(self):
        assert GUID.cache_ok is True

    def test_process_bind_param_none(self):
        guid = GUID()
        assert guid.process_bind_param(None, _MockDialect("sqlite")) is None

    def test_process_bind_param_uuid_to_string(self):
        guid = GUID()
        test_uuid = uuid.uuid4()
        result = guid.process_bind_param(test_uuid, _MockDialect("sqlite"))
        assert isinstance(result, str)
        assert result == str(test_uuid)

    def test_process_bind_param_uuid_native_pg(self):
        guid = GUID()
        test_uuid = uuid.uuid4()
        result = guid.process_bind_param(test_uuid, _MockDialect("postgresql"))
        assert isinstance(result, uuid.UUID)
        assert result == test_uuid

    def test_process_bind_param_string_to_uuid(self):
        guid = GUID()
        test_uuid = uuid.uuid4()
        result = guid.process_bind_param(str(test_uuid), _MockDialect("postgresql"))
        assert isinstance(result, uuid.UUID)

    def test_process_result_value_none(self):
        guid = GUID()
        assert guid.process_result_value(None, _MockDialect("sqlite")) is None

    def test_process_result_value_uuid_passthrough(self):
        guid = GUID()
        test_uuid = uuid.uuid4()
        result = guid.process_result_value(test_uuid, _MockDialect("sqlite"))
        assert result == test_uuid

    def test_process_result_value_string_to_uuid(self):
        guid = GUID()
        test_uuid = uuid.uuid4()
        result = guid.process_result_value(str(test_uuid), _MockDialect("sqlite"))
        assert isinstance(result, uuid.UUID)
        assert result == test_uuid


class _MockDialect:
    def __init__(self, name: str) -> None:
        self.name = name
