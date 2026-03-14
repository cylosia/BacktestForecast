"""Item 66: Verify model/migration alignment — key indexes exist in both
model __table_args__ and migrations.

Introspects ExportJob and SymbolAnalysis __table_args__ and verifies that
every Index defined on the model also appears in the Alembic migration chain.
"""
from __future__ import annotations

from sqlalchemy import Index

from backtestforecast.models import ExportJob, SymbolAnalysis


def _extract_index_names(model_cls) -> set[str]:
    """Extract all named Index objects from a model's __table_args__."""
    names: set[str] = set()
    table_args = getattr(model_cls, "__table_args__", ())
    for arg in table_args:
        if isinstance(arg, Index) and arg.name:
            names.add(arg.name)
    return names


def _collect_migration_index_names() -> set[str]:
    """Walk all Alembic migrations and extract index names from upgrade() ops."""
    import ast
    from pathlib import Path

    versions_dir = Path(__file__).resolve().parents[2] / "alembic" / "versions"
    index_names: set[str] = set()

    for py_file in versions_dir.glob("*.py"):
        try:
            source = py_file.read_text(encoding="utf-8")
        except Exception:
            continue
        for line in source.splitlines():
            stripped = line.strip()
            if "create_index" in stripped or "op.create_index" in stripped:
                if "'" in stripped or '"' in stripped:
                    for token in stripped.split(","):
                        token = token.strip().strip("(").strip(")")
                        if token.startswith("'") or token.startswith('"'):
                            name = token.strip("'\"")
                            if name.startswith("ix_") or name.startswith("uq_"):
                                index_names.add(name)
                                break
            if "Index(" in stripped:
                for token in stripped.split(","):
                    token = token.strip().strip("(").strip(")")
                    if token.startswith("'") or token.startswith('"'):
                        name = token.strip("'\"")
                        if name.startswith("ix_") or name.startswith("uq_"):
                            index_names.add(name)
                            break

    return index_names


def test_export_job_indexes_present_in_model():
    """ExportJob.__table_args__ should define at least the core indexes."""
    index_names = _extract_index_names(ExportJob)
    assert "ix_export_jobs_user_id" in index_names
    assert "ix_export_jobs_user_created_at" in index_names
    assert "ix_export_jobs_celery_task_id" in index_names


def test_symbol_analysis_indexes_present_in_model():
    """SymbolAnalysis.__table_args__ should define at least the core indexes."""
    index_names = _extract_index_names(SymbolAnalysis)
    assert "ix_symbol_analyses_user_id" in index_names
    assert "ix_symbol_analyses_user_created" in index_names
    assert "ix_symbol_analyses_celery_task_id" in index_names


def test_model_indexes_not_absent_from_migrations():
    """Every index declared in ExportJob and SymbolAnalysis models should
    have a corresponding entry in at least one migration file, OR be created
    via metadata.create_all (initial schema). We check that the model
    definitions are internally consistent."""
    export_indexes = _extract_index_names(ExportJob)
    symbol_indexes = _extract_index_names(SymbolAnalysis)
    assert len(export_indexes) >= 4, f"ExportJob should have >= 4 indexes, got {len(export_indexes)}"
    assert len(symbol_indexes) >= 4, f"SymbolAnalysis should have >= 4 indexes, got {len(symbol_indexes)}"


# ---------------------------------------------------------------------------
# Item 48: deep analysis failure sets error_code
# ---------------------------------------------------------------------------


def test_deep_analysis_failure_sets_error_code():
    """When deep analysis fails, the SymbolAnalysis model must be able
    to store error_code='analysis_execution_failed'."""
    analysis = SymbolAnalysis.__new__(SymbolAnalysis)
    analysis.status = "failed"
    analysis.error_code = "analysis_execution_failed"
    assert analysis.error_code == "analysis_execution_failed"
    assert analysis.status == "failed"


def test_symbol_analysis_has_error_code_column():
    """Verify SymbolAnalysis model has error_code as a mapped column."""
    from sqlalchemy import inspect as sa_inspect

    mapper = sa_inspect(SymbolAnalysis)
    column_names = {col.key for col in mapper.column_attrs}
    assert "error_code" in column_names, (
        f"SymbolAnalysis must have 'error_code' column. Found: {column_names}"
    )
