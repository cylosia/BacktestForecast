from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (ROOT / relpath).read_text(encoding="utf-8")


def test_daily_picks_repository_and_service_expose_real_total() -> None:
    repo_source = _read("src/backtestforecast/repositories/daily_picks.py")
    service_source = _read("src/backtestforecast/services/daily_picks.py")

    assert "def count_pipeline_history" in repo_source
    assert "parse_cursor_param(cursor)" in service_source
    assert "finalize_cursor_page(runs, total=total, offset=offset, limit=effective_limit)" in service_source
    assert '"total": page.total' in service_source
    assert '"limit": page.limit' in service_source


def test_scan_and_sweep_persistence_do_not_use_len_gt_50_truncation_heuristic() -> None:
    scans_source = _read("src/backtestforecast/services/scans.py")
    sweeps_source = _read("src/backtestforecast/services/sweeps.py")
    adapters_source = _read("src/backtestforecast/pipeline/adapters.py")

    assert '"trades_truncated": len(execution_result.trades) > 50' not in scans_source
    assert "trades_truncated = len(result.trades) > 50" not in sweeps_source
    assert '"trades_truncated": len(result.trades) > 50' not in adapters_source
    assert '"trade_count": full_trade_count' in sweeps_source
    assert '"serialized_trade_count": serialized_trade_count' in sweeps_source
    assert '"trade_items_omitted": max(trade_count - serialized_trade_count, 0)' in adapters_source
    assert '"trades_truncated": full_trade_count > serialized_trade_count' not in sweeps_source
    assert '"trades_truncated": candidate.get("trades_truncated", False)' not in sweeps_source
    assert 'ranking_with_meta["trades_truncated"]' not in scans_source
