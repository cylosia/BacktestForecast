"""Verification tests for the Dead Code / Confusing Code / Refactor Targets.

Each test confirms the cleanup is in place.
"""
from __future__ import annotations

import inspect
import warnings

import pytest


# ---- DC1: hasattr(run, 'trade_count') removed ----

def test_dc1_no_hasattr_trade_count():
    from backtestforecast.services.exports import ExportService
    source = inspect.getsource(ExportService.execute_export_by_id)
    assert "hasattr(run" not in source


# ---- DC2: _validate_evaluated_count now rejects ----

def test_dc2_evaluated_count_raises_on_extreme():
    from backtestforecast.models import ScannerJob
    source = inspect.getsource(ScannerJob._validate_evaluated_count)
    assert "ValueError" in source
    assert "_MAX_EVAL_MULTIPLIER" in source


# ---- DC3: CONTRACT_MULTIPLIER getattr extracted to _leg_multiplier ----

def test_dc3_leg_multiplier_helper_exists():
    from backtestforecast.backtests.engine import _leg_multiplier
    assert callable(_leg_multiplier)


def test_dc3_engine_uses_leg_multiplier():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine)
    assert "_leg_multiplier(leg)" in source
    raw_count = source.count('getattr(leg, "contract_multiplier"')
    assert raw_count == 0, f"Found {raw_count} raw getattr calls; should use _leg_multiplier"


def test_dc3_leg_multiplier_returns_default():
    from backtestforecast.backtests.engine import _leg_multiplier, CONTRACT_MULTIPLIER

    class FakeLeg:
        pass

    assert _leg_multiplier(FakeLeg()) == CONTRACT_MULTIPLIER


def test_dc3_leg_multiplier_returns_custom():
    from backtestforecast.backtests.engine import _leg_multiplier

    class FakeLeg:
        contract_multiplier = 50.0

    assert _leg_multiplier(FakeLeg()) == 50.0


# ---- DC4: _ModelT TypeVar documented ----

def test_dc4_model_typevar_has_comment():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        import apps.worker.app.tasks
    source = inspect.getsource(apps.worker.app.tasks)
    assert "_ModelT" in source
    idx = source.index("_ModelT = TypeVar")
    end = source.index("\n", idx)
    line = source[idx:end]
    assert "#" in line or "Used by" in line


# ---- DC5: dead outerjoin import removed ----

def test_dc5_no_outerjoin_import():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import cleanup_daily_recommendations
    source = inspect.getsource(cleanup_daily_recommendations)
    assert "from sqlalchemy import outerjoin" not in source


# ---- DC6: _LOOKS_NUMERIC at module level (acceptable) ----

def test_dc6_looks_numeric_at_module_level():
    from backtestforecast.services.exports import _LOOKS_NUMERIC
    assert _LOOKS_NUMERIC is not None
    assert _LOOKS_NUMERIC.match("42")
    assert not _LOOKS_NUMERIC.match("007")


# ---- DC7: task_helpers.py exists for factored-out code ----

def test_dc7_task_helpers_module_exists():
    from apps.worker.app.task_helpers import commit_then_publish
    assert callable(commit_then_publish)


# ---- DC8: _commit_then_publish imports from task_helpers ----

def test_dc8_tasks_imports_from_helpers():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        import apps.worker.app.tasks as tasks_mod
    source_lines = inspect.getsource(tasks_mod).split("\n")
    import_line = [l for l in source_lines if "commit_then_publish" in l and "import" in l]
    assert len(import_line) >= 1, "tasks.py should import _commit_then_publish from task_helpers"
    assert "task_helpers" in import_line[0]


def test_dc8_no_duplicate_definition_in_tasks():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        import apps.worker.app.tasks as tasks_mod
    source = inspect.getsource(tasks_mod)
    def_count = source.count("def _commit_then_publish(")
    assert def_count == 0, f"Found {def_count} definitions; should be imported not defined"
    def_count_2 = source.count("def commit_then_publish(")
    assert def_count_2 == 0, "commit_then_publish should be imported, not redefined"
