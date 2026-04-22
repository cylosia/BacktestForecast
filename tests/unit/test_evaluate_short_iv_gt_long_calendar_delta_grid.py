from __future__ import annotations

from datetime import date
from pathlib import Path
import shutil
import uuid

import scripts.evaluate_short_iv_gt_long_calendar_delta_grid as module


def _make_local_temp_dir() -> Path:
    path = module.ROOT / "logs" / f"test_delta_grid_cache_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_default_symbol_cache_dir_uses_output_prefix_name() -> None:
    temp_dir = _make_local_temp_dir()
    output_prefix = temp_dir / "short_iv_gt_long_calendar_delta_grid_part1"

    try:
        cache_dir = module._default_symbol_cache_dir(output_prefix)

        assert cache_dir == temp_dir / "short_iv_gt_long_calendar_delta_grid_part1_symbol_cache"
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_symbol_result_cache_round_trip() -> None:
    temp_dir = _make_local_temp_dir()
    cache_dir = temp_dir / "cache"
    metadata = module._symbol_cache_metadata(
        symbol="AAPL",
        method_name="mlgbp72",
        entry_start_date=date(2024, 4, 19),
        entry_end_date=date(2026, 4, 10),
        explicit_entry_dates=set(),
        short_dte_max=10,
        gap_dte_max=10,
        delta_targets=(50, 45, 40),
    )
    result = module.SymbolEvaluationResult(
        symbol="AAPL",
        weekly_candidate_rows=[{"symbol": "AAPL", "entry_date": "2026-04-17"}],
        detail_rows=[{"symbol": "AAPL", "entry_date": "2026-04-17", "pnl": 1.23}],
        status_message="AAPL: built predictions",
    )

    try:
        module._store_cached_symbol_result(cache_dir=cache_dir, metadata=metadata, result=result)
        cached = module._load_cached_symbol_result(cache_dir=cache_dir, metadata=metadata)

        assert cached is not None
        assert cached.symbol == "AAPL"
        assert cached.weekly_candidate_rows == result.weekly_candidate_rows
        assert cached.detail_rows == result.detail_rows
        assert "reused symbol cache" in cached.status_message
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_symbol_result_cache_skips_mismatched_metadata() -> None:
    temp_dir = _make_local_temp_dir()
    cache_dir = temp_dir / "cache"
    metadata = module._symbol_cache_metadata(
        symbol="AAPL",
        method_name="mlgbp72",
        entry_start_date=date(2024, 4, 19),
        entry_end_date=date(2026, 4, 10),
        explicit_entry_dates=set(),
        short_dte_max=10,
        gap_dte_max=10,
        delta_targets=(50, 45, 40),
    )
    changed_metadata = dict(metadata)
    changed_metadata["method_name"] = "vote40rsi"
    result = module.SymbolEvaluationResult(
        symbol="AAPL",
        weekly_candidate_rows=[],
        detail_rows=[],
        status_message="AAPL: built predictions",
    )

    try:
        module._store_cached_symbol_result(cache_dir=cache_dir, metadata=metadata, result=result)

        assert module._load_cached_symbol_result(cache_dir=cache_dir, metadata=changed_metadata) is None
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
