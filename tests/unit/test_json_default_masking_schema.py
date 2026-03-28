from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (ROOT / relpath).read_text(encoding="utf-8")


def test_symbol_analysis_json_fields_are_nullable_without_hidden_defaults() -> None:
    source = _read("src/backtestforecast/models.py")

    assert 'regime_json: Mapped[dict[str, Any] | None] = mapped_column(JSON_VARIANT, nullable=True)' in source
    assert 'landscape_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON_VARIANT, nullable=True)' in source
    assert 'top_results_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON_VARIANT, nullable=True)' in source
    assert 'forecast_json: Mapped[dict[str, Any] | None] = mapped_column(JSON_VARIANT, nullable=True)' in source


def test_scan_and_sweep_result_json_fields_no_longer_hide_missing_writes() -> None:
    source = _read("src/backtestforecast/models.py")

    assert 'request_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)' in source
    assert 'summary_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)' in source
    assert 'warnings_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False)' in source
    assert 'trades_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False)' in source
    assert 'equity_curve_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False)' in source


def test_alembic_removes_masking_json_server_defaults() -> None:
    source = _read("alembic/versions/20260324_0001_consolidated_baseline.py")

    assert "POSTGRESQL_DDL_STATEMENTS" in source
    assert "scanner_recommendations" in source
    assert "sweep_results" in source
    assert "symbol_analyses" in source
