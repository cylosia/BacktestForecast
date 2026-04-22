from __future__ import annotations

import csv
import subprocess
import sys
import uuid
from pathlib import Path

import scripts.build_short_iv_gt_long_best_delta_selected_trades as module


def _make_local_temp_dir() -> Path:
    path = module.LOGS / f"test_best_delta_selected_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_grid_selected_builder_filters_to_best_delta_rows() -> None:
    temp_dir = _make_local_temp_dir()
    try:
        best_delta_csv = temp_dir / "best_delta.csv"
        grid_a_csv = temp_dir / "grid_a.csv"
        grid_b_csv = temp_dir / "grid_b.csv"
        output_trades_csv = temp_dir / "selected.csv"
        output_summary_csv = temp_dir / "summary.csv"

        _write_csv(
            best_delta_csv,
            ["symbol", "best_up_delta_target_pct", "best_abstain_delta_target_pct"],
            [
                {"symbol": "AAA", "best_up_delta_target_pct": "45", "best_abstain_delta_target_pct": "50"},
                {"symbol": "BBB", "best_up_delta_target_pct": "50", "best_abstain_delta_target_pct": ""},
            ],
        )
        fieldnames = [
            "symbol",
            "entry_date",
            "prediction",
            "selected_method",
            "prediction_engine",
            "confidence_pct",
            "delta_target_pct",
            "entry_debit",
            "spread_mark",
            "pnl",
            "roi_pct",
            "short_strike",
            "short_expiration",
            "long_expiration",
            "spot_close_entry",
            "spot_close_mark",
            "short_mark_method",
            "long_mark_method",
            "nonpositive_debit_flag",
        ]
        _write_csv(
            grid_a_csv,
            fieldnames,
            [
                {
                    "symbol": "AAA",
                    "entry_date": "2026-03-20",
                    "prediction": "up",
                    "selected_method": "m1",
                    "prediction_engine": "ml",
                    "confidence_pct": "70",
                    "delta_target_pct": "45",
                    "entry_debit": "1.1",
                    "spread_mark": "1.4",
                    "pnl": "0.3",
                    "roi_pct": "27.27",
                    "short_strike": "100",
                    "short_expiration": "2026-03-27",
                    "long_expiration": "2026-04-02",
                    "spot_close_entry": "101",
                    "spot_close_mark": "102",
                    "short_mark_method": "exact",
                    "long_mark_method": "exact",
                    "nonpositive_debit_flag": "0",
                },
                {
                    "symbol": "AAA",
                    "entry_date": "2026-03-20",
                    "prediction": "up",
                    "selected_method": "m1",
                    "prediction_engine": "ml",
                    "confidence_pct": "70",
                    "delta_target_pct": "50",
                    "entry_debit": "1.2",
                    "spread_mark": "1.3",
                    "pnl": "0.1",
                    "roi_pct": "8.33",
                    "short_strike": "101",
                    "short_expiration": "2026-03-27",
                    "long_expiration": "2026-04-02",
                    "spot_close_entry": "101",
                    "spot_close_mark": "102",
                    "short_mark_method": "exact",
                    "long_mark_method": "exact",
                    "nonpositive_debit_flag": "0",
                },
            ],
        )
        _write_csv(
            grid_b_csv,
            fieldnames,
            [
                {
                    "symbol": "AAA",
                    "entry_date": "2026-04-02",
                    "prediction": "abstain",
                    "selected_method": "m2",
                    "prediction_engine": "analog",
                    "confidence_pct": "",
                    "delta_target_pct": "50",
                    "entry_debit": "0.9",
                    "spread_mark": "1.0",
                    "pnl": "0.1",
                    "roi_pct": "11.11",
                    "short_strike": "98",
                    "short_expiration": "2026-04-10",
                    "long_expiration": "2026-04-17",
                    "spot_close_entry": "99",
                    "spot_close_mark": "100",
                    "short_mark_method": "exact",
                    "long_mark_method": "exact",
                    "nonpositive_debit_flag": "0",
                },
                {
                    "symbol": "BBB",
                    "entry_date": "2026-03-20",
                    "prediction": "up",
                    "selected_method": "m3",
                    "prediction_engine": "ml",
                    "confidence_pct": "80",
                    "delta_target_pct": "50",
                    "entry_debit": "2.0",
                    "spread_mark": "2.4",
                    "pnl": "0.4",
                    "roi_pct": "20.0",
                    "short_strike": "110",
                    "short_expiration": "2026-03-27",
                    "long_expiration": "2026-04-02",
                    "spot_close_entry": "111",
                    "spot_close_mark": "112",
                    "short_mark_method": "exact",
                    "long_mark_method": "exact",
                    "nonpositive_debit_flag": "0",
                },
            ],
        )

        cmd = [
            sys.executable,
            str(module.ROOT / "scripts" / "build_short_iv_gt_long_best_delta_selected_trades.py"),
            "--best-delta-csv",
            str(best_delta_csv),
            "--grid-trades-csv",
            str(grid_a_csv),
            str(grid_b_csv),
            "--entry-dates",
            "2026-03-20",
            "2026-04-02",
            "--output-trades-csv",
            str(output_trades_csv),
            "--output-summary-csv",
            str(output_summary_csv),
        ]
        subprocess.run(cmd, check=True, cwd=module.ROOT)

        with output_trades_csv.open(encoding="utf-8", newline="") as fh:
            selected_rows = list(csv.DictReader(fh))
        with output_summary_csv.open(encoding="utf-8", newline="") as fh:
            summary_rows = list(csv.DictReader(fh))

        assert len(selected_rows) == 3
        assert {(row["entry_date"], row["symbol"], row["prediction"], row["best_delta_target_pct"]) for row in selected_rows} == {
            ("2026-03-20", "AAA", "up", "45"),
            ("2026-03-20", "BBB", "up", "50"),
            ("2026-04-02", "AAA", "abstain", "50"),
        }
        assert len(summary_rows) == 4
        abstain_2026_03_20 = next(
            row for row in summary_rows if row["entry_date"] == "2026-03-20" and row["prediction"] == "abstain"
        )
        assert abstain_2026_03_20["added_trade_count"] == "0"
    finally:
        for path in temp_dir.glob("**/*"):
            if path.is_file():
                path.unlink()
        for path in sorted((p for p in temp_dir.glob("**/*") if p.is_dir()), reverse=True):
            path.rmdir()
        temp_dir.rmdir()
