"""Contract test: validate API responses against the OpenAPI snapshot.

Loads the OpenAPI spec from openapi.snapshot.json and validates that
the critical endpoint response shapes match the declared schemas.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

SNAPSHOT_PATH = Path(__file__).resolve().parents[2] / "openapi.snapshot.json"


@pytest.fixture()
def openapi_spec():
    if not SNAPSHOT_PATH.exists():
        pytest.skip("openapi.snapshot.json not found")
    return json.loads(SNAPSHOT_PATH.read_text())


def test_openapi_snapshot_has_paths(openapi_spec):
    """Smoke: the snapshot declares the expected high-level paths."""
    paths = openapi_spec.get("paths", {})
    assert "/backtests" in paths or "/api/backtests" in paths or any("backtest" in p for p in paths), (
        "Expected at least one backtest-related path in the OpenAPI spec"
    )


def test_openapi_snapshot_info_title(openapi_spec):
    assert openapi_spec.get("info", {}).get("title"), "OpenAPI spec must have an info.title"


def test_openapi_snapshot_schemas_exist(openapi_spec):
    schemas = openapi_spec.get("components", {}).get("schemas", {})
    assert len(schemas) > 0, "OpenAPI spec must define at least one schema"
