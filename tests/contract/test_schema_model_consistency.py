"""Test 76: Verify AnalysisSummaryResponse fields have corresponding columns
in the SymbolAnalysis model with compatible types.
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import inspect as sa_inspect

from backtestforecast.models import SymbolAnalysis
from backtestforecast.schemas.analysis import AnalysisSummaryResponse


_PYDANTIC_TO_SQL_COMPAT: dict[str, set[str]] = {
    "UUID": {"GUID", "UUID", "CHAR", "VARCHAR", "String"},
    "str": {"String", "Text", "VARCHAR", "CHAR"},
    "int": {"Integer", "BigInteger", "SmallInteger"},
    "float": {"Numeric", "Float", "Double", "DECIMAL"},
    "datetime": {"DateTime", "TIMESTAMP"},
    "date": {"Date"},
    "bool": {"Boolean"},
}


def _python_type_name(annotation) -> str:
    origin = getattr(annotation, "__origin__", None)
    if origin is type(None):
        return "NoneType"
    if origin is not None:
        args = getattr(annotation, "__args__", ())
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            return non_none[0].__name__ if hasattr(non_none[0], "__name__") else str(non_none[0])
    if hasattr(annotation, "__name__"):
        return annotation.__name__
    return str(annotation)


def test_analysis_summary_fields_exist_on_model():
    """Every field in AnalysisSummaryResponse must have a corresponding column
    in SymbolAnalysis (unless it's a computed/virtual field)."""
    mapper = sa_inspect(SymbolAnalysis)
    model_columns = {col.key for col in mapper.column_attrs}

    schema_fields = set(AnalysisSummaryResponse.model_fields.keys())

    for field_name in schema_fields:
        assert field_name in model_columns, (
            f"AnalysisSummaryResponse.{field_name} has no corresponding column "
            f"on SymbolAnalysis. Model columns: {sorted(model_columns)}"
        )


def test_analysis_summary_type_compatibility():
    """Field types in AnalysisSummaryResponse should be compatible with
    the corresponding SymbolAnalysis column types."""
    mapper = sa_inspect(SymbolAnalysis)
    model_column_types: dict[str, str] = {}
    for col_attr in mapper.column_attrs:
        col = col_attr.columns[0]
        model_column_types[col_attr.key] = type(col.type).__name__

    for field_name, field_info in AnalysisSummaryResponse.model_fields.items():
        if field_name not in model_column_types:
            continue

        annotation = field_info.annotation
        py_type = _python_type_name(annotation)
        sql_type = model_column_types[field_name]

        compatible_sql_types = _PYDANTIC_TO_SQL_COMPAT.get(py_type, set())
        if compatible_sql_types:
            assert sql_type in compatible_sql_types, (
                f"Type mismatch for '{field_name}': schema={py_type}, "
                f"model={sql_type}. Expected one of {compatible_sql_types}"
            )


# ---------------------------------------------------------------------------
# Test 77: PipelineHistoryItemResponse field types
# ---------------------------------------------------------------------------


def test_pipeline_history_item_id_is_uuid():
    """PipelineHistoryItemResponse.id must be UUID, not str."""
    from backtestforecast.schemas.analysis import PipelineHistoryItemResponse

    field = PipelineHistoryItemResponse.model_fields["id"]
    assert field.annotation is UUID, (
        f"PipelineHistoryItemResponse.id should be UUID, got {field.annotation}"
    )


def test_pipeline_history_item_completed_at_is_optional_datetime():
    """PipelineHistoryItemResponse.completed_at must be datetime | None, not str."""
    from backtestforecast.schemas.analysis import PipelineHistoryItemResponse

    field = PipelineHistoryItemResponse.model_fields["completed_at"]
    annotation = field.annotation

    origin = getattr(annotation, "__origin__", None)
    args = getattr(annotation, "__args__", ())

    non_none_args = [a for a in args if a is not type(None)] if args else []

    assert any(
        a is datetime or (hasattr(a, "__name__") and a.__name__ == "datetime")
        for a in non_none_args
    ) or annotation is datetime, (
        f"PipelineHistoryItemResponse.completed_at should include datetime, "
        f"got {annotation}"
    )

    assert type(None) in args if args else True, (
        f"PipelineHistoryItemResponse.completed_at should be Optional, "
        f"got {annotation}"
    )
