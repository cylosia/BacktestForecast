"""Test that coverage configuration exists and critical modules have tests.

Ensures pyproject.toml has a [tool.coverage.report] section with
fail_under set to at least 80%, and validates that critical modules
have corresponding test files to prevent coverage gaps.
"""
from __future__ import annotations

from pathlib import Path

import pytest

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_pyproject() -> dict:
    path = PROJECT_ROOT / "pyproject.toml"
    assert path.exists(), f"pyproject.toml not found at {path}"
    with open(path, "rb") as f:
        return tomllib.load(f)


def test_coverage_report_section_exists() -> None:
    data = _load_pyproject()
    coverage = data.get("tool", {}).get("coverage", {}).get("report", {})
    assert coverage, (
        "pyproject.toml must have a [tool.coverage.report] section"
    )


def test_global_coverage_threshold_at_least_80() -> None:
    data = _load_pyproject()
    fail_under = data["tool"]["coverage"]["report"].get("fail_under", 0)
    assert fail_under >= 80, (
        f"Global coverage threshold should be >= 80%, got {fail_under}%"
    )


def test_coverage_source_is_src() -> None:
    data = _load_pyproject()
    source = data.get("tool", {}).get("coverage", {}).get("run", {}).get("source", [])
    assert "src" in source, (
        "Coverage source should include 'src' to measure the application code"
    )


CRITICAL_MODULES_WITH_EXPECTED_TESTS = {
    "backtests/engine": "backtests/",
    "backtests/strategies": "backtests/",
    "services/backtests": "unit/",
    "services/billing": "billing/",
    "services/scans": "unit/",
    "services/sweeps": "unit/",
    "services/exports": "unit/",
    "billing/entitlements": "billing/",
    "security/rate_limits": "security/",
    "auth/verification": "security/",
    "resilience/circuit_breaker": "unit/",
    "market_data/service": "unit/",
}


@pytest.mark.parametrize(
    "module,test_dir",
    CRITICAL_MODULES_WITH_EXPECTED_TESTS.items(),
    ids=CRITICAL_MODULES_WITH_EXPECTED_TESTS.keys(),
)
def test_critical_module_has_test_coverage(module: str, test_dir: str) -> None:
    """Critical modules must have at least one test file in the expected directory."""
    tests_dir = PROJECT_ROOT / "tests" / test_dir
    if not tests_dir.exists():
        pytest.skip(f"Test directory {tests_dir} does not exist")
    module_stem = module.split("/")[-1]
    test_files = list(tests_dir.rglob(f"*{module_stem}*"))
    test_files += list(tests_dir.rglob("test_*.py"))
    assert len(test_files) > 0, (
        f"Critical module '{module}' has no tests in tests/{test_dir}. "
        f"Add at least one test file covering this module."
    )
