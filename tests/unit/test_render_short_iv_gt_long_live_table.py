from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import render_short_iv_gt_long_live_table as table_script  # noqa: E402


def test_format_option_spread_uses_single_symbol_prefix() -> None:
    assert (
        table_script._format_option_spread(
            "O:XRT260424C00087500",
            "O:XRT260501C00087500",
        )
        == "XRT 260424 C 87.50 - 260501 C 87.50"
    )


def test_build_display_row_uses_method_side_tp_sl_overrides() -> None:
    row = {
        "symbol": "XRT",
        "prediction": "abstain",
        "selected_method": "mlgbp72",
        "best_delta_target_pct": "45",
        "entry_debit": "0.24",
        "short_option_ticker": "O:XRT260424C00087500",
        "long_option_ticker": "O:XRT260501C00087500",
    }

    assert table_script._build_display_row(row) == {
        "symbol": "XRT",
        "delta": "45",
        "option_spread": "XRT 260424 C 87.50 - 260501 C 87.50",
        "entry_debit": "0.24",
        "TP": "0",
        "SL": "65",
    }


def test_build_display_row_uses_upside_tp_sl_for_up_predictions() -> None:
    row = {
        "symbol": "ABC",
        "prediction": "up",
        "selected_method": "median25trend",
        "best_delta_target_pct": "40",
        "entry_debit": "1.50",
        "short_option_ticker": "O:ABC260424C00100000",
        "long_option_ticker": "O:ABC260501C00100000",
    }

    assert table_script._build_display_row(row)["TP"] == "75"
    assert table_script._build_display_row(row)["SL"] == "65"


def test_default_input_csv_accepts_top43_files(monkeypatch) -> None:
    class _FakeStat:
        def __init__(self, mtime: float) -> None:
            self.st_mtime = mtime

    class _FakePath:
        def __init__(self, name: str, mtime: float) -> None:
            self.name = name
            self._mtime = mtime

        def stat(self) -> _FakeStat:
            return _FakeStat(self._mtime)

    older = _FakePath("short_iv_gt_long_live_top40_2026-04-17.csv", 100.0)
    newer = _FakePath("short_iv_gt_long_live_top43_2026-04-17.csv", 200.0)

    class _FakeLogs:
        def glob(self, pattern: str) -> list[_FakePath]:
            assert pattern == "short_iv_gt_long_live_top*.csv"
            return [older, newer]

    monkeypatch.setattr(table_script, "LOGS", _FakeLogs())

    assert table_script._default_input_csv() is newer
