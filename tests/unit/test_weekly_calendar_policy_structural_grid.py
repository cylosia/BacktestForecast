from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

from backtestforecast.backtests.types import RiskFreeRateCurve
from backtestforecast.schemas.backtests import StrategyType

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from scripts.grid_search_fas_faz_weekly_calendar_policy import StrategyConfig
from scripts.run_weekly_calendar_policy_structural_grid import (
    StructuralVariant,
    _build_structural_calendar_config,
    _build_structural_variants,
    _parse_int_grid,
    _parse_optional_int_grid,
    _resolve_adjustment_policy,
    _select_entry_dates,
    _trade_to_ledger_row,
    _variant_run_label,
)


def test_parse_int_grid_preserves_order() -> None:
    assert _parse_int_grid("5, 7,9") == (5, 7, 9)


def test_parse_optional_int_grid_accepts_blank() -> None:
    assert _parse_optional_int_grid("") == ()


def test_build_structural_variants_filters_invalid_pairs() -> None:
    variants = _build_structural_variants(
        short_dtes=(7, 9),
        far_dtes=(7, 14, 21),
        max_holding_days_grid=(5,),
    )
    assert [item.label for item in variants] == [
        "sdte7_fdte14_hold5",
        "sdte7_fdte21_hold5",
        "sdte9_fdte14_hold5",
        "sdte9_fdte21_hold5",
    ]


def test_build_structural_variants_crosses_profit_target_overrides() -> None:
    variants = _build_structural_variants(
        short_dtes=(7,),
        far_dtes=(14,),
        max_holding_days_grid=(5,),
        profit_target_pcts=(10, 20),
    )
    assert [item.label for item in variants] == [
        "sdte7_fdte14_hold5_pt10",
        "sdte7_fdte14_hold5_pt20",
    ]


def test_build_structural_calendar_config_applies_variant_to_call_strategy() -> None:
    strategy = StrategyConfig(
        label="spy_call_d40_pt50",
        symbol="SPY",
        strategy_type=StrategyType.CALENDAR_SPREAD,
        delta_target=40,
        profit_target_pct=50,
    )
    config = _build_structural_calendar_config(
        strategy=strategy,
        entry_date=date(2026, 1, 2),
        latest_available_date=date(2026, 1, 20),
        risk_free_curve=RiskFreeRateCurve(default_rate=0.04),
        variant=StructuralVariant(short_dte=9, far_leg_target_dte=21, max_holding_days=7, profit_target_pct=30),
        dte_tolerance_days=2,
    )

    assert config.target_dte == 9
    assert config.max_holding_days == 7
    assert config.dte_tolerance_days == 2
    assert config.profit_target_pct == 30.0
    assert config.strategy_overrides is not None
    assert config.strategy_overrides.calendar_far_leg_target_dte == 21
    assert config.strategy_overrides.short_call_strike is not None
    assert config.strategy_overrides.short_call_strike.value == 40


def test_build_structural_calendar_config_applies_variant_to_put_strategy() -> None:
    strategy = StrategyConfig(
        label="bear_spy_put_d30_pt75",
        symbol="SPY",
        strategy_type=StrategyType.PUT_CALENDAR_SPREAD,
        delta_target=30,
        profit_target_pct=75,
    )
    config = _build_structural_calendar_config(
        strategy=strategy,
        entry_date=date(2026, 1, 2),
        latest_available_date=date(2026, 1, 20),
        risk_free_curve=RiskFreeRateCurve(default_rate=0.04),
        variant=StructuralVariant(short_dte=5, far_leg_target_dte=14, max_holding_days=5, profit_target_pct=20),
        dte_tolerance_days=3,
    )

    assert config.target_dte == 5
    assert config.max_holding_days == 5
    assert config.profit_target_pct == 20.0
    assert config.strategy_overrides is not None
    assert config.strategy_overrides.calendar_far_leg_target_dte == 14
    assert config.strategy_overrides.short_put_strike is not None
    assert config.strategy_overrides.short_put_strike.value == 30


def test_resolve_adjustment_policy_and_variant_label() -> None:
    policy = _resolve_adjustment_policy("roll_same_strike_once")
    assert policy is not None
    variant = StructuralVariant(short_dte=7, far_leg_target_dte=21, max_holding_days=10)
    assert _variant_run_label(variant, policy) == "sdte7_fdte21_hold10_roll_same_strike_once"


def test_trade_to_ledger_row_prefers_campaign_max_capital_at_risk() -> None:
    variant = StructuralVariant(short_dte=7, far_leg_target_dte=21, max_holding_days=10, profit_target_pct=20)
    candidate = {
        "rank": 1,
        "symbol": "SPY",
        "weight_pct": 8.0,
        "position_multiplier": 1.5,
        "best": {"median_roi_on_margin_pct": 80.0, "trade_count": 100},
    }
    trade = SimpleNamespace(
        quantity=2,
        option_ticker="TEST",
        entry_date=date(2026, 1, 2),
        exit_date=date(2026, 1, 9),
        entry_underlying_close=500.0,
        exit_underlying_close=505.0,
        exit_reason="expiration",
        net_pnl=100.0,
        detail_json={
            "entry_package_market_value": 250.0,
            "capital_required_per_unit": 300.0,
            "campaign_max_capital_at_risk": 900.0,
            "campaign_adjustment_events": [{"event_type": "roll_same_strike_once"}],
            "campaign_roll_count": 1,
        },
    )

    row = _trade_to_ledger_row(
        variant=variant,
        candidate=candidate,
        trade=trade,
        regime="bullish",
        strategy_label="test",
        adjustment_policy=_resolve_adjustment_policy("roll_same_strike_once"),
    )

    assert row["variant_label"] == "sdte7_fdte21_hold10_pt20_roll_same_strike_once"
    assert row["profit_target_pct"] == 20
    assert row["strategy"] == "test_pt20"
    assert row["capital_required"] == 900.0
    assert row["adjustment_event_count"] == 1
    assert row["campaign_roll_count"] == 1


def test_select_entry_dates_supports_daily_and_weekly() -> None:
    bars = [
        SimpleNamespace(trade_date=date(2026, 1, 1)),
        SimpleNamespace(trade_date=date(2026, 1, 2)),
        SimpleNamespace(trade_date=date(2026, 1, 5)),
    ]

    daily = _select_entry_dates(
        bars=bars,
        entry_start_date=date(2026, 1, 1),
        entry_end_date=date(2026, 1, 5),
        entry_cadence="daily",
    )
    weekly = _select_entry_dates(
        bars=bars,
        entry_start_date=date(2026, 1, 1),
        entry_end_date=date(2026, 1, 5),
        entry_cadence="weekly",
    )

    assert daily == [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 5)]
    assert weekly == [date(2026, 1, 2)]
