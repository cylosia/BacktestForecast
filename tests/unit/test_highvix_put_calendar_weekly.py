from __future__ import annotations

import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from backtestforecast.backtests.types import RiskFreeRateCurve

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import grid_search_highVIX_uvix_uvxy_vxx_put_calendar_weekly as grid  # noqa: E402
import run_highVIX_uvix_uvxy_vxx_put_calendar_weekly as runner  # noqa: E402


def test_parse_symbols_normalizes_case_and_deduplicates() -> None:
    assert runner._parse_symbols(" uvix,UVXY, uvix , vxx ") == ("UVIX", "UVXY", "VXX")


def test_build_calendar_config_applies_put_calendar_settings() -> None:
    config = runner._build_calendar_config(
        strategy=runner.StrategyConfig(symbol="UVXY", delta_target=40, profit_target_pct=75.0),
        entry_date=date(2026, 1, 2),
        replay_data_end=date(2026, 1, 25),
        risk_free_curve=RiskFreeRateCurve(default_rate=0.04),
    )

    assert config.symbol == "UVXY"
    assert config.strategy_type == runner.StrategyType.PUT_CALENDAR_SPREAD.value
    assert config.target_dte == runner.TARGET_DTE
    assert config.max_holding_days == runner.MAX_HOLDING_DAYS
    assert config.profit_target_pct == 75.0
    assert config.strategy_overrides is not None
    assert config.strategy_overrides.calendar_far_leg_target_dte == runner.FAR_LEG_TARGET_DTE
    assert config.strategy_overrides.short_put_strike is not None
    assert config.strategy_overrides.short_put_strike.mode == runner.StrikeSelectionMode.DELTA_TARGET
    assert config.strategy_overrides.short_put_strike.value == Decimal("40")


def test_extract_short_put_strike_from_trade_returns_short_put_only() -> None:
    trade = SimpleNamespace(
        detail_json={
            "legs": [
                {"asset_type": "option", "side": "long", "contract_type": "put", "strike_price": 15},
                {"asset_type": "option", "side": "short", "contract_type": "call", "strike_price": 20},
                {"asset_type": "option", "side": "short", "contract_type": "put", "strike_price": 17.5},
            ]
        }
    )

    assert runner._extract_short_put_strike_from_trade(trade) == 17.5


def test_trade_roi_on_margin_pct_uses_quantity_adjusted_capital() -> None:
    trade = SimpleNamespace(
        detail_json={"capital_required_per_unit": 250.0},
        quantity=2,
        net_pnl=50.0,
    )

    assert runner._trade_roi_on_margin_pct(trade) == 10.0


def test_summarize_records_and_yearly_breakdown_aggregate_pnl_and_roi() -> None:
    records = [
        {"entry_date": "2025-01-03", "net_pnl": 100.0, "roi_on_margin_pct": 10.0},
        {"entry_date": "2025-02-07", "net_pnl": -20.0, "roi_on_margin_pct": -2.0},
        {"entry_date": "2026-01-09", "net_pnl": 30.0, "roi_on_margin_pct": 3.0},
    ]

    summary = runner._summarize_records(records)
    yearly = runner._yearly_breakdown(records)

    assert summary["trade_count"] == 3
    assert summary["total_net_pnl"] == 110.0
    assert summary["ending_equity"] == 100110.0
    assert summary["win_rate_pct"] == 66.6667
    assert summary["median_roi_on_margin_pct"] == 3.0
    assert yearly == [
        {
            "year": "2025",
            "trade_count": 2,
            "net_pnl": 80.0,
            "roi_pct": 0.08,
            "average_roi_on_margin_pct": 4.0,
            "median_roi_on_margin_pct": 4.0,
        },
        {
            "year": "2026",
            "trade_count": 1,
            "net_pnl": 30.0,
            "roi_pct": 0.03,
            "average_roi_on_margin_pct": 3.0,
            "median_roi_on_margin_pct": 3.0,
        },
    ]


def test_parse_int_values_preserves_order_and_deduplicates() -> None:
    assert grid._parse_int_values("50, 30,50,40") == (50, 30, 40)


def test_compute_weekly_median_rows_groups_non_null_roi_values() -> None:
    rows = [
        {"entry_date": "2026-01-02", "roi_on_margin_pct": 10.0},
        {"entry_date": "2026-01-02", "roi_on_margin_pct": 20.0},
        {"entry_date": "2026-01-09", "roi_on_margin_pct": None},
        {"entry_date": "2026-01-09", "roi_on_margin_pct": 30.0},
    ]

    assert grid._compute_weekly_median_rows(rows) == [
        {
            "entry_date": "2026-01-02",
            "trade_count": 2,
            "median_roi_on_margin_pct": 15.0,
            "roi_values": [10.0, 20.0],
        },
        {
            "entry_date": "2026-01-09",
            "trade_count": 1,
            "median_roi_on_margin_pct": 30.0,
            "roi_values": [30.0],
        },
    ]


def test_grid_row_rank_key_prioritizes_median_roi_then_tiebreakers() -> None:
    strong_roi = {
        "median_roi_on_margin_pct": 12.0,
        "total_net_pnl": 10.0,
        "win_rate_pct": 40.0,
        "trade_count": 5,
    }
    better_tiebreaker = {
        "median_roi_on_margin_pct": 10.0,
        "total_net_pnl": 20.0,
        "win_rate_pct": 70.0,
        "trade_count": 8,
    }
    weaker_tiebreaker = {
        "median_roi_on_margin_pct": 10.0,
        "total_net_pnl": 15.0,
        "win_rate_pct": 80.0,
        "trade_count": 10,
    }

    assert grid._grid_row_rank_key(strong_roi) > grid._grid_row_rank_key(better_tiebreaker)
    assert grid._grid_row_rank_key(better_tiebreaker) > grid._grid_row_rank_key(weaker_tiebreaker)
