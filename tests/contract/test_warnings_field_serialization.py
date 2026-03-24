"""Contract test: warnings field uses field name 'warnings', not alias 'warnings_json'.

The DB column is ``warnings_json``, but the Pydantic response schemas expose it
as ``warnings`` via ``validation_alias``.  Serialised JSON (sent to the
frontend) must use the field name ``warnings``, never the alias.
"""
from __future__ import annotations

import pytest

from backtestforecast.schemas.backtests import BacktestRunDetailResponse
from backtestforecast.schemas.scans import ScannerJobResponse
from backtestforecast.schemas.sweeps import SweepJobResponse

_SCHEMAS_WITH_WARNINGS = [
    BacktestRunDetailResponse,
    ScannerJobResponse,
    SweepJobResponse,
]


@pytest.mark.parametrize("schema_cls", _SCHEMAS_WITH_WARNINGS, ids=lambda c: c.__name__)
def test_warnings_field_exists_with_correct_alias(schema_cls) -> None:
    """Each schema must have a 'warnings' field with validation_alias='warnings_json'."""
    fields = schema_cls.model_fields
    assert "warnings" in fields, f"{schema_cls.__name__} must have a 'warnings' field"
    field_info = fields["warnings"]
    assert field_info.validation_alias == "warnings_json", (
        f"{schema_cls.__name__}.warnings should have validation_alias='warnings_json', "
        f"got {field_info.validation_alias!r}"
    )


@pytest.mark.parametrize("schema_cls", _SCHEMAS_WITH_WARNINGS, ids=lambda c: c.__name__)
def test_warnings_serialises_as_warnings_not_alias(schema_cls) -> None:
    """Serialised output must use 'warnings' as the key, not 'warnings_json'."""
    by_alias_fields = set()
    for name, field_info in schema_cls.model_fields.items():
        serialization_alias = field_info.serialization_alias
        by_alias_fields.add(serialization_alias or name)

    assert "warnings" in by_alias_fields or "warnings" in schema_cls.model_fields, (
        f"{schema_cls.__name__} serialisation must include 'warnings'"
    )
    assert "warnings_json" not in by_alias_fields, (
        f"{schema_cls.__name__} must NOT serialise as 'warnings_json'"
    )
