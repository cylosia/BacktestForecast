"""Test 75: Verify CheckConstraint names don't contain duplicated prefixes.

After applying the naming convention, no CheckConstraint name should have
a doubled prefix like ``ck_users_valid_plan_tier_valid_plan_tier``.
"""
from __future__ import annotations

import re

from sqlalchemy import CheckConstraint

from backtestforecast.models import (
    AuditEvent,
    BacktestEquityPoint,
    BacktestRun,
    BacktestTemplate,
    BacktestTrade,
    DailyRecommendation,
    ExportJob,
    NightlyPipelineRun,
    ScannerJob,
    ScannerRecommendation,
    SymbolAnalysis,
    User,
)

_ALL_MODELS = [
    User,
    BacktestRun,
    BacktestTrade,
    BacktestEquityPoint,
    BacktestTemplate,
    ScannerJob,
    ScannerRecommendation,
    ExportJob,
    AuditEvent,
    NightlyPipelineRun,
    DailyRecommendation,
    SymbolAnalysis,
]


def _extract_check_constraint_names(model_cls) -> list[str]:
    names: list[str] = []
    table_args = getattr(model_cls, "__table_args__", ())
    for arg in table_args:
        if isinstance(arg, CheckConstraint) and arg.name:
            names.append(arg.name)
    return names


def test_no_doubled_prefixes_in_check_constraints():
    """No CheckConstraint name should contain a duplicated segment."""
    for model in _ALL_MODELS:
        table_name = model.__tablename__
        for name in _extract_check_constraint_names(model):
            if name.startswith("ck_"):
                prefix = f"ck_{table_name}_"
                if name.startswith(prefix):
                    rest = name[len(prefix):]
                    assert not rest.startswith(prefix), (
                        f"CheckConstraint '{name}' on {table_name} "
                        f"has doubled prefix"
                    )


def test_check_constraint_names_are_reasonable_length():
    """No CheckConstraint name should exceed 63 chars (PostgreSQL limit)."""
    for model in _ALL_MODELS:
        for name in _extract_check_constraint_names(model):
            assert len(name) <= 63, (
                f"CheckConstraint '{name}' on {model.__tablename__} "
                f"exceeds PostgreSQL's 63-char identifier limit (len={len(name)})"
            )


def test_all_check_constraints_have_names():
    """Every CheckConstraint must have an explicit name for migration stability."""
    for model in _ALL_MODELS:
        table_args = getattr(model, "__table_args__", ())
        for arg in table_args:
            if isinstance(arg, CheckConstraint):
                assert arg.name, (
                    f"Unnamed CheckConstraint found on {model.__tablename__}: "
                    f"{arg.sqltext}"
                )


def test_check_constraint_naming_consistency():
    """All CheckConstraints must follow the ``ck_<tablename>_<descriptor>`` convention."""
    for model in _ALL_MODELS:
        table_name = model.__tablename__
        expected_prefix = f"ck_{table_name}_"
        for name in _extract_check_constraint_names(model):
            assert name.startswith(expected_prefix), (
                f"CheckConstraint '{name}' on {table_name} does not follow "
                f"the naming convention '{expected_prefix}<descriptor>'"
            )
