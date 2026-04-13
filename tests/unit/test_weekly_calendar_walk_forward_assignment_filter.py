from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import run_weekly_calendar_policy_walk_forward as walk_forward


def _write_candidate_payload(path: Path, *, median_roi: float, total_roi: float, trade_count: int) -> None:
    path.write_text(
        json.dumps(
            {
                "period": {
                    "start": "2024-01-01",
                    "requested_end": "2025-12-31",
                    "latest_available_date": "2025-12-31",
                },
                "combined_best_result": {
                    "roc_period": 63,
                    "adx_period": 14,
                    "rsi_period": 14,
                    "bull_filter": "roc0_adx10_rsinone",
                    "bear_filter": "roc0_adx14_rsinone",
                    "bull_strategy": "aaa_call_d40_pt50",
                    "bear_strategy": "bear_aaa_call_d30_pt50",
                    "neutral_strategy": "neutral_aaa_call_d40_pt50",
                    "trade_count": trade_count,
                    "total_net_pnl": 1000.0,
                    "total_roi_pct": total_roi,
                    "average_roi_on_margin_pct": median_roi - 5.0,
                    "median_roi_on_margin_pct": median_roi,
                    "win_rate_pct": 60.0,
                    "average_win": 100.0,
                    "average_loss": -50.0,
                },
            }
        ),
        encoding="utf-8",
    )


def test_passes_assignment_filters_respects_put_assignment_thresholds() -> None:
    metrics = {
        "training_assignment_count": 3,
        "training_assignment_rate_pct": 5.0,
        "training_put_assignment_count": 2,
        "training_put_assignment_rate_pct": 3.0,
    }
    assert walk_forward._passes_assignment_filters(
        metrics=metrics,
        max_training_assignment_count=3,
        max_training_assignment_rate_pct=5.0,
        max_training_put_assignment_count=2,
        max_training_put_assignment_rate_pct=3.0,
    )
    assert not walk_forward._passes_assignment_filters(
        metrics=metrics,
        max_training_assignment_count=2,
        max_training_assignment_rate_pct=None,
        max_training_put_assignment_count=None,
        max_training_put_assignment_rate_pct=None,
    )
    assert not walk_forward._passes_assignment_filters(
        metrics=metrics,
        max_training_assignment_count=None,
        max_training_assignment_rate_pct=None,
        max_training_put_assignment_count=1,
        max_training_put_assignment_rate_pct=None,
    )


def test_load_candidates_filters_assignment_risk_and_attaches_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    root = Path("C:/assignment-filter-test-root")
    monkeypatch.setattr(walk_forward, "ROOT", root)

    payload_a = root / "aaa.json"
    payload_b = root / "bbb.json"
    payload_text_by_path: dict[Path, str] = {}
    for path, median_roi, total_roi in (
        (payload_a, 90.0, 10.0),
        (payload_b, 95.0, 12.0),
    ):
        payload_text_by_path[path] = json.dumps(
            {
                "period": {
                    "start": "2024-01-01",
                    "requested_end": "2025-12-31",
                    "latest_available_date": "2025-12-31",
                },
                "combined_best_result": {
                    "roc_period": 63,
                    "adx_period": 14,
                    "rsi_period": 14,
                    "bull_filter": "roc0_adx10_rsinone",
                    "bear_filter": "roc0_adx14_rsinone",
                    "bull_strategy": "aaa_call_d40_pt50",
                    "bear_strategy": "bear_aaa_call_d30_pt50",
                    "neutral_strategy": "neutral_aaa_call_d40_pt50",
                    "trade_count": 100,
                    "total_net_pnl": 1000.0,
                    "total_roi_pct": total_roi,
                    "average_roi_on_margin_pct": median_roi - 5.0,
                    "median_roi_on_margin_pct": median_roi,
                    "win_rate_pct": 60.0,
                    "average_win": 100.0,
                    "average_loss": -50.0,
                },
            }
        )

    real_exists = Path.exists
    real_read_text = Path.read_text

    def _fake_exists(self: Path) -> bool:
        return self in payload_text_by_path or real_exists(self)

    def _fake_read_text(self: Path, *args, **kwargs) -> str:
        if self in payload_text_by_path:
            return payload_text_by_path[self]
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "exists", _fake_exists)
    monkeypatch.setattr(Path, "read_text", _fake_read_text)

    summary_buffer = io.StringIO()
    writer = csv.DictWriter(
        summary_buffer,
        fieldnames=[
            "symbol",
            "status",
            "objective",
            "trade_count",
            "output_path",
            "start_date",
            "requested_end_date",
        ],
    )
    writer.writeheader()
    writer.writerow(
        {
            "symbol": "AAA",
            "status": "completed",
            "objective": "median",
            "trade_count": "100",
            "output_path": "aaa.json",
            "start_date": "2024-01-01",
            "requested_end_date": "2025-12-31",
        }
    )
    writer.writerow(
        {
            "symbol": "BBB",
            "status": "completed",
            "objective": "median",
            "trade_count": "100",
            "output_path": "bbb.json",
            "start_date": "2024-01-01",
            "requested_end_date": "2025-12-31",
        }
    )
    summary_text = summary_buffer.getvalue()

    class _SummaryPath:
        def open(self, *args, **kwargs):
            return io.StringIO(summary_text)

    metrics_by_symbol = {
        "AAA": {
            "training_assignment_count": 0,
            "training_assignment_rate_pct": 0.0,
            "training_put_assignment_count": 0,
            "training_put_assignment_rate_pct": 0.0,
        },
        "BBB": {
            "training_assignment_count": 4,
            "training_assignment_rate_pct": 4.0,
            "training_put_assignment_count": 2,
            "training_put_assignment_rate_pct": 2.0,
        },
    }
    monkeypatch.setattr(
        walk_forward,
        "_load_candidate_training_assignment_metrics",
        lambda candidate: metrics_by_symbol[str(candidate["symbol"])],
    )

    candidates, stats = walk_forward._load_candidates(
        summary_csv=_SummaryPath(),
        train_objective="median",
        min_trade_count=70,
        min_median_roi=None,
        max_training_assignment_count=0,
        max_training_assignment_rate_pct=None,
        max_training_put_assignment_count=None,
        max_training_put_assignment_rate_pct=None,
    )

    assert [candidate["symbol"] for candidate in candidates] == ["AAA"]
    assert candidates[0]["training_assignment_metrics"] == metrics_by_symbol["AAA"]
    assert stats == {
        "base_candidate_count": 2,
        "assignment_filtered_out_count": 1,
    }
