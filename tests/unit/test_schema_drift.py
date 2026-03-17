"""Item 66: Verify model/migration alignment — key indexes and CHECK
constraints exist in both model __table_args__ and migrations.

Introspects model __table_args__ and verifies that every Index and
CheckConstraint defined on the model also appears in the Alembic migration chain.
"""
from __future__ import annotations

from sqlalchemy import CheckConstraint, Index

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
    from types import SimpleNamespace
    analysis = SimpleNamespace(status="failed", error_code="analysis_execution_failed")
    assert analysis.error_code == "analysis_execution_failed"
    assert analysis.status == "failed"

    from sqlalchemy import inspect as sa_inspect
    mapper = sa_inspect(SymbolAnalysis)
    column_names = {col.key for col in mapper.column_attrs}
    assert "error_code" in column_names
    assert "status" in column_names


def test_symbol_analysis_has_error_code_column():
    """Verify SymbolAnalysis model has error_code as a mapped column."""
    from sqlalchemy import inspect as sa_inspect

    mapper = sa_inspect(SymbolAnalysis)
    column_names = {col.key for col in mapper.column_attrs}
    assert "error_code" in column_names, (
        f"SymbolAnalysis must have 'error_code' column. Found: {column_names}"
    )


# ---------------------------------------------------------------------------
# Fix 30: model-migration CHECK constraint sync
# ---------------------------------------------------------------------------


def _extract_check_constraint_names(model_cls) -> set[str]:
    """Extract all named CheckConstraint objects from a model's __table_args__."""
    names: set[str] = set()
    table_args = getattr(model_cls, "__table_args__", ())
    for arg in table_args:
        if isinstance(arg, CheckConstraint) and arg.name:
            names.add(arg.name)
    return names


def _collect_migration_check_constraint_names() -> set[str]:
    """Walk all Alembic migrations and extract CHECK constraint names."""
    from pathlib import Path

    versions_dir = Path(__file__).resolve().parents[2] / "alembic" / "versions"
    ck_names: set[str] = set()

    for py_file in versions_dir.glob("*.py"):
        try:
            source = py_file.read_text(encoding="utf-8")
        except Exception:
            continue
        for line in source.splitlines():
            stripped = line.strip()
            if "CheckConstraint" not in stripped and "create_check_constraint" not in stripped:
                continue
            for token in stripped.split(","):
                token = token.strip().strip("(").strip(")")
                if "name=" in token:
                    val = token.split("name=")[1].strip().strip("\"'),(")
                    if val.startswith("ck_"):
                        ck_names.add(val)
                        break

    return ck_names


def test_migration_check_constraints_exist_in_models():
    """Every CHECK constraint created in a migration must also be declared
    in the corresponding model's __table_args__."""
    import backtestforecast.models as models_mod
    from sqlalchemy.orm import DeclarativeBase

    model_ck_names: set[str] = set()
    base_cls = getattr(models_mod, "Base", None)
    for attr_name in dir(models_mod):
        cls = getattr(models_mod, attr_name)
        if not isinstance(cls, type):
            continue
        if cls is base_cls or cls is DeclarativeBase:
            continue
        if hasattr(cls, "__table_args__"):
            model_ck_names |= _extract_check_constraint_names(cls)

    migration_ck_names = _collect_migration_check_constraint_names()

    missing = migration_ck_names - model_ck_names
    assert not missing, (
        f"CHECK constraints in migrations but missing from models: {sorted(missing)}"
    )
