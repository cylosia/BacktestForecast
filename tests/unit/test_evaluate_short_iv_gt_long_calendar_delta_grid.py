from __future__ import annotations

from datetime import date
from pathlib import Path
import shutil
import uuid

import pytest

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
        entry_weekday=module.DEFAULT_ENTRY_WEEKDAY,
        short_dte_max=10,
        gap_dte_max=10,
        short_expiration_dte_targets=(),
        long_expiration_dte_targets=(),
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
        entry_weekday=module.DEFAULT_ENTRY_WEEKDAY,
        short_dte_max=10,
        gap_dte_max=10,
        short_expiration_dte_targets=(),
        long_expiration_dte_targets=(),
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


def test_select_weekly_calendar_candidates_skips_earnings_before_short_expiration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry_date = date(2026, 4, 17)
    short_expiration = date(2026, 4, 24)
    long_expiration = date(2026, 5, 1)
    strike = 100.0
    spot = 101.0
    option_rows_by_date = {
        entry_date: {
            short_expiration: [
                module.OptionRow("short", entry_date, short_expiration, strike, 2.0),
            ],
            long_expiration: [
                module.OptionRow("long", entry_date, long_expiration, strike, 3.0),
            ],
        }
    }

    monkeypatch.setattr(module, "choose_atm_strike", lambda strikes, _: strike)
    monkeypatch.setattr(
        module,
        "_estimate_call_iv_pct",
        lambda **kwargs: 40.0 if kwargs["expiration_date"] == short_expiration else 30.0,
    )

    candidates = module._select_weekly_calendar_candidates(
        symbol="URI",
        entry_dates=[entry_date],
        spot_by_date={entry_date: spot},
        option_rows_by_date=option_rows_by_date,
        short_dte_max=10,
        gap_dte_max=10,
        earnings_dates={date(2026, 4, 22)},
    )

    assert candidates == []


def test_select_weekly_calendar_candidates_allows_earnings_on_short_expiration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry_date = date(2026, 4, 17)
    short_expiration = date(2026, 4, 24)
    long_expiration = date(2026, 5, 1)
    strike = 100.0
    spot = 101.0
    option_rows_by_date = {
        entry_date: {
            short_expiration: [
                module.OptionRow("short", entry_date, short_expiration, strike, 2.0),
            ],
            long_expiration: [
                module.OptionRow("long", entry_date, long_expiration, strike, 3.0),
            ],
        }
    }

    monkeypatch.setattr(module, "choose_atm_strike", lambda strikes, _: strike)
    monkeypatch.setattr(
        module,
        "_estimate_call_iv_pct",
        lambda **kwargs: 40.0 if kwargs["expiration_date"] == short_expiration else 30.0,
    )

    candidates = module._select_weekly_calendar_candidates(
        symbol="URI",
        entry_dates=[entry_date],
        spot_by_date={entry_date: spot},
        option_rows_by_date=option_rows_by_date,
        short_dte_max=10,
        gap_dte_max=10,
        earnings_dates={short_expiration},
    )

    assert len(candidates) == 1
    assert candidates[0].symbol == "URI"


def test_select_weekly_calendar_candidates_can_target_deferred_thursday_expirations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry_date = date(2026, 4, 16)
    same_week_expiration = date(2026, 4, 17)
    short_expiration = date(2026, 4, 24)
    long_expiration = date(2026, 5, 1)
    strike = 100.0
    spot = 101.0
    option_rows_by_date = {
        entry_date: {
            same_week_expiration: [
                module.OptionRow("same_week", entry_date, same_week_expiration, strike, 1.0),
            ],
            short_expiration: [
                module.OptionRow("short", entry_date, short_expiration, strike, 2.0),
            ],
            long_expiration: [
                module.OptionRow("long", entry_date, long_expiration, strike, 3.0),
            ],
        }
    }

    monkeypatch.setattr(module, "choose_atm_strike", lambda strikes, _: strike)
    monkeypatch.setattr(
        module,
        "_estimate_call_iv_pct",
        lambda **kwargs: 40.0 if kwargs["expiration_date"] == short_expiration else 30.0,
    )

    candidates = module._select_weekly_calendar_candidates(
        symbol="URI",
        entry_dates=[entry_date],
        spot_by_date={entry_date: spot},
        option_rows_by_date=option_rows_by_date,
        short_dte_max=10,
        gap_dte_max=10,
        short_expiration_dte_targets=(7, 8),
        long_expiration_dte_targets=(14, 15),
    )

    assert len(candidates) == 1
    assert candidates[0].short_expiration == short_expiration
    assert candidates[0].long_expiration == long_expiration
