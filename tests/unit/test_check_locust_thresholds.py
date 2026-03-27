from __future__ import annotations

import csv
import io
from pathlib import Path
from unittest.mock import patch

from scripts import check_locust_thresholds


def _write_stats_csv(
    *,
    requests: str = "10",
    failures: str = "0",
    p95_ms: str = "1500",
) -> str:
    handle = io.StringIO()
    writer = csv.DictWriter(
        handle,
        fieldnames=["Name", "Request Count", "Failure Count", "95%"],
    )
    writer.writeheader()
    writer.writerow(
        {
            "Name": "Aggregated",
            "Request Count": requests,
            "Failure Count": failures,
            "95%": p95_ms,
        }
    )
    return handle.getvalue()


def _patch_stats_csv(csv_text: str):
    return patch.object(Path, "open", return_value=io.StringIO(csv_text))


def test_check_locust_thresholds_passes_within_limits(monkeypatch) -> None:
    csv_text = _write_stats_csv()
    csv_path = Path("artifacts/locust_stats.csv")
    monkeypatch.setenv("LOCUST_STATS_CSV", str(csv_path))
    monkeypatch.setenv("LOCUST_MIN_REQUESTS", "1")
    monkeypatch.setenv("LOCUST_MAX_FAILURES", "0")
    monkeypatch.setenv("LOCUST_MAX_P95_MS", "2000")

    with patch.object(Path, "exists", return_value=True), _patch_stats_csv(csv_text):
        assert check_locust_thresholds.main() == 0


def test_check_locust_thresholds_fails_when_no_requests(monkeypatch) -> None:
    csv_text = _write_stats_csv(requests="0")
    csv_path = Path("artifacts/locust_stats.csv")
    monkeypatch.setenv("LOCUST_STATS_CSV", str(csv_path))
    monkeypatch.setenv("LOCUST_MIN_REQUESTS", "1")

    with patch.object(Path, "exists", return_value=True), _patch_stats_csv(csv_text):
        assert check_locust_thresholds.main() == 1


def test_check_locust_thresholds_fails_when_latency_exceeds_limit(monkeypatch) -> None:
    csv_text = _write_stats_csv(p95_ms="2500")
    csv_path = Path("artifacts/locust_stats.csv")
    monkeypatch.setenv("LOCUST_STATS_CSV", str(csv_path))
    monkeypatch.setenv("LOCUST_MAX_P95_MS", "2000")

    with patch.object(Path, "exists", return_value=True), _patch_stats_csv(csv_text):
        assert check_locust_thresholds.main() == 1
