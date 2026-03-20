"""Test that ExportJobResponse uses ExportFormat enum, not regex string.

Regression test for the contract mismatch where adding a new ExportFormat
value would require updating both the enum AND a separate regex pattern.
"""
from __future__ import annotations

from backtestforecast.billing.entitlements import ExportFormat
from backtestforecast.schemas.exports import ExportJobResponse


def test_export_format_field_type_is_enum():
    """ExportJobResponse.export_format must use the ExportFormat enum."""
    field = ExportJobResponse.model_fields["export_format"]
    assert field.annotation is ExportFormat, (
        f"ExportJobResponse.export_format should be ExportFormat enum, "
        f"got {field.annotation}"
    )


def test_export_format_coerces_string_to_enum():
    """A string value like 'csv' must be coerced to ExportFormat.CSV."""
    from datetime import datetime, UTC
    from uuid import uuid4
    r = ExportJobResponse(
        id=uuid4(),
        backtest_run_id=uuid4(),
        export_format="csv",
        status="succeeded",
        file_name="test.csv",
        mime_type="text/csv",
        created_at=datetime.now(UTC),
    )
    assert r.export_format == ExportFormat.CSV
    assert isinstance(r.export_format, ExportFormat)


def test_export_format_pdf_coercion():
    from datetime import datetime, UTC
    from uuid import uuid4
    r = ExportJobResponse(
        id=uuid4(),
        backtest_run_id=uuid4(),
        export_format="pdf",
        status="succeeded",
        file_name="test.pdf",
        mime_type="application/pdf",
        created_at=datetime.now(UTC),
    )
    assert r.export_format == ExportFormat.PDF


def test_export_format_invalid_rejected():
    from datetime import datetime, UTC
    from uuid import uuid4
    from pydantic import ValidationError
    import pytest
    with pytest.raises(ValidationError):
        ExportJobResponse(
            id=uuid4(),
            backtest_run_id=uuid4(),
            export_format="xlsx",
            status="succeeded",
            file_name="test.xlsx",
            mime_type="application/vnd.openxmlformats",
            created_at=datetime.now(UTC),
        )
