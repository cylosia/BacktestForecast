from __future__ import annotations

import json
from pathlib import Path

import pytest

SNAPSHOT_PATH = Path(__file__).resolve().parents[2] / "openapi.snapshot.json"


@pytest.fixture(scope="module")
def snapshot_components() -> dict[str, dict]:
    if not SNAPSHOT_PATH.exists():
        pytest.skip("openapi.snapshot.json not found")
    snapshot = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    return snapshot.get("components", {}).get("schemas", {})


def _schema_props(components: dict[str, dict], name: str) -> tuple[str, ...]:
    return tuple(sorted(components[name].get("properties", {}).keys()))


def test_backtest_detail_payload_snapshot(snapshot_components: dict[str, dict]) -> None:
    assert _schema_props(snapshot_components, "BacktestRunDetailResponse") == (
        "account_size",
        "commission_per_contract",
        "completed_at",
        "created_at",
        "data_source",
        "date_from",
        "date_to",
        "dte_tolerance_days",
        "engine_version",
        "equity_curve",
        "equity_curve_truncated",
        "error_code",
        "error_message",
        "id",
        "max_holding_days",
        "risk_free_rate",
        "risk_per_trade_pct",
        "started_at",
        "status",
        "strategy_type",
        "summary",
        "symbol",
        "target_dte",
        "trades",
        "warnings",
    )


def test_compare_payload_snapshot(snapshot_components: dict[str, dict]) -> None:
    assert _schema_props(snapshot_components, "CompareBacktestsResponse") == (
        "comparison_limit",
        "items",
        "trade_limit_per_run",
        "trades_truncated",
    )


def test_export_job_payload_snapshot(snapshot_components: dict[str, dict]) -> None:
    assert _schema_props(snapshot_components, "ExportJobResponse") == (
        "backtest_run_id",
        "completed_at",
        "created_at",
        "error_code",
        "error_message",
        "expires_at",
        "export_format",
        "file_name",
        "id",
        "mime_type",
        "sha256_hex",
        "size_bytes",
        "started_at",
        "status",
    )


def test_account_export_payload_snapshot(snapshot_components: dict[str, dict]) -> None:
    assert _schema_props(snapshot_components, "AccountDataExportResponse") == (
        "audit_events",
        "backtests",
        "export_jobs",
        "pagination",
        "scanner_jobs",
        "sweep_jobs",
        "symbol_analyses",
        "templates",
        "totals",
        "user",
    )
