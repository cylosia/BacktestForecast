from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import evaluate_short_iv_gt_long_conditional_management_3weeks as module  # noqa: E402


def test_best_combined_policy_label_points_to_preferred_filtered_variant() -> None:
    assert (
        module.BEST_COMBINED_POLICY_LABEL
        == module.BEST_COMBINED_MEDIAN25TREND_MLLOGREG56_FILTER_POLICY_LABEL
    )


def test_should_apply_first_breach_exit_requires_actual_tested_exit() -> None:
    assert module._should_apply_first_breach_exit(
        first_breach_row={"exit_reason": "expiration", "roi_pct": 25.0},
        is_eligible=True,
    ) is False


def test_should_apply_first_breach_exit_applies_tp0_sl35_bounds() -> None:
    assert module._should_apply_first_breach_exit(
        first_breach_row={"exit_reason": "spot_close_above_short_strike", "roi_pct": 5.0},
        is_eligible=True,
    ) is True
    assert module._should_apply_first_breach_exit(
        first_breach_row={"exit_reason": "spot_close_above_short_strike", "roi_pct": -40.0},
        is_eligible=True,
    ) is True
    assert module._should_apply_first_breach_exit(
        first_breach_row={"exit_reason": "spot_close_above_short_strike", "roi_pct": -10.0},
        is_eligible=True,
    ) is False


def test_should_apply_piecewise_abstain_tp25_stop35_matches_target_ranges() -> None:
    assert module._should_apply_piecewise_abstain_tp25_stop35(
        prediction="abstain",
        entry_debit=7.25,
        short_entry_iv_pct=42.49,
    ) is True
    assert module._should_apply_piecewise_abstain_tp25_stop35(
        prediction="abstain",
        entry_debit=3.40,
        short_entry_iv_pct=62.81,
    ) is True
    assert module._should_apply_piecewise_abstain_tp25_stop35(
        prediction="abstain",
        entry_debit=4.21,
        short_entry_iv_pct=29.62,
    ) is False
    assert module._should_apply_piecewise_abstain_tp25_stop35(
        prediction="up",
        entry_debit=7.25,
        short_entry_iv_pct=42.49,
    ) is False


def test_should_apply_midhigh_iv_tested_exit_excludes_piecewise_ranges() -> None:
    assert module._should_apply_midhigh_iv_tested_exit(
        prediction="abstain",
        entry_debit=2.75,
        short_entry_iv_pct=72.0,
        already_piecewise_managed=False,
    ) is True
    assert module._should_apply_midhigh_iv_tested_exit(
        prediction="abstain",
        entry_debit=3.40,
        short_entry_iv_pct=62.81,
        already_piecewise_managed=True,
    ) is False
    assert module._should_apply_midhigh_iv_tested_exit(
        prediction="abstain",
        entry_debit=2.25,
        short_entry_iv_pct=72.0,
        already_piecewise_managed=False,
    ) is False


def test_derive_symbol_side_lookback_filtered_rows_uses_side_specific_base_history() -> None:
    rows = [
        {"entry_date": "2025-01-03", "symbol": "ABC", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": -1.0},
        {"entry_date": "2025-01-10", "symbol": "ABC", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 2.0},
        {"entry_date": "2025-01-17", "symbol": "ABC", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 3.0},
        {"entry_date": "2025-01-24", "symbol": "ABC", "prediction": "up", "policy_label": "base", "entry_debit": 2.0, "pnl": -4.0},
        {"entry_date": "2025-01-31", "symbol": "ABC", "prediction": "up", "policy_label": "base", "entry_debit": 2.0, "pnl": 5.0},
    ]

    filtered = module._derive_symbol_side_lookback_filtered_rows(
        rows=rows,
        source_policy_label="base",
        derived_policy_label="filtered",
    )

    assert [(row["entry_date"], row["prediction"]) for row in filtered] == [
        ("2025-01-03", "abstain"),
        ("2025-01-17", "abstain"),
        ("2025-01-24", "up"),
    ]


def test_derive_symbol_side_lookback_filtered_rows_ignores_nonpositive_debit_history() -> None:
    rows = [
        {"entry_date": "2025-01-03", "symbol": "ABC", "prediction": "abstain", "policy_label": "base", "entry_debit": -1.0, "pnl": -10.0},
        {"entry_date": "2025-01-10", "symbol": "ABC", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 1.0},
    ]

    filtered = module._derive_symbol_side_lookback_filtered_rows(
        rows=rows,
        source_policy_label="base",
        derived_policy_label="filtered",
    )

    assert [(row["entry_date"], row["prediction"]) for row in filtered] == [
        ("2025-01-03", "abstain"),
        ("2025-01-10", "abstain"),
    ]


def test_derive_symbol_side_lookback_filtered_rows_expires_history_outside_52_weeks() -> None:
    rows = [
        {"entry_date": "2025-01-03", "symbol": "ABC", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": -5.0},
        {"entry_date": "2026-01-08", "symbol": "ABC", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 1.0},
    ]

    filtered = module._derive_symbol_side_lookback_filtered_rows(
        rows=rows,
        source_policy_label="base",
        derived_policy_label="filtered",
    )

    assert [(row["entry_date"], row["prediction"]) for row in filtered] == [
        ("2025-01-03", "abstain"),
        ("2026-01-08", "abstain"),
    ]


def test_derive_targeted_best_combined_variant_rows_skips_only_bad_up_bucket_methods() -> None:
    rows = [
        {
            "entry_date": "2025-01-03",
            "symbol": "AAA",
            "prediction": "up",
            "policy_label": module.BASE_BEST_COMBINED_POLICY_LABEL,
            "entry_debit": 2.0,
            "pnl": -1.0,
            "confidence_pct": 72.0,
            "selected_method": "mllogreg56",
        },
        {
            "entry_date": "2025-01-03",
            "symbol": "BBB",
            "prediction": "up",
            "policy_label": module.BASE_BEST_COMBINED_POLICY_LABEL,
            "entry_debit": 2.5,
            "pnl": 1.5,
            "confidence_pct": 72.0,
            "selected_method": "median15trend",
        },
        {
            "entry_date": "2025-01-03",
            "symbol": "CCC",
            "prediction": "abstain",
            "policy_label": module.BASE_BEST_COMBINED_POLICY_LABEL,
            "entry_debit": 3.0,
            "pnl": 0.5,
            "confidence_pct": "",
            "selected_method": "mlgbp68",
        },
    ]

    filtered = module._derive_targeted_best_combined_variant_rows(
        rows=rows,
        source_policy_label=module.BASE_BEST_COMBINED_POLICY_LABEL,
        derived_policy_label="derived",
    )

    assert [(row["symbol"], row["prediction"]) for row in filtered] == [
        ("BBB", "up"),
        ("CCC", "abstain"),
    ]
    assert all(row["policy_label"] == "derived" for row in filtered)
    assert all(row["position_size_weight"] == 1.0 for row in filtered)
    assert all(row["position_sizing_rule"] == "" for row in filtered)


def test_derive_targeted_best_combined_variant_rows_half_sizes_expensive_abstain_trades() -> None:
    rows = [
        {
            "entry_date": "2025-01-10",
            "symbol": "ABC",
            "prediction": "abstain",
            "policy_label": module.BASE_BEST_COMBINED_POLICY_LABEL,
            "original_entry_debit": 6.0,
            "entry_debit": 5.0,
            "spread_mark": 1.5,
            "pnl": -2.0,
            "roll_net_debit": 0.4,
            "roi_pct": -40.0,
            "confidence_pct": "",
            "selected_method": "mlgbp64",
        }
    ]

    filtered = module._derive_targeted_best_combined_variant_rows(
        rows=rows,
        source_policy_label=module.BASE_BEST_COMBINED_POLICY_LABEL,
        derived_policy_label="derived",
        abstain_half_size_entry_debit_threshold=4.0,
    )

    assert len(filtered) == 1
    half_sized = filtered[0]
    assert half_sized["policy_label"] == "derived"
    assert half_sized["position_size_weight"] == 0.5
    assert half_sized["position_sizing_rule"] == "half_size_abstain_entry_debit_gt_4"
    assert half_sized["original_entry_debit"] == 3.0
    assert half_sized["entry_debit"] == 2.5
    assert half_sized["spread_mark"] == 0.75
    assert half_sized["pnl"] == -1.0
    assert half_sized["roll_net_debit"] == 0.2
    assert half_sized["roi_pct"] == -40.0


def test_derive_skip_filtered_policy_rows_preserves_existing_position_size_metadata() -> None:
    rows = [
        {
            "entry_date": "2025-01-10",
            "symbol": "AAA",
            "prediction": "abstain",
            "policy_label": module.BEST_COMBINED_POLICY_LABEL,
            "entry_debit": 2.5,
            "pnl": -1.0,
            "position_size_weight": 0.5,
            "position_sizing_rule": "half_size_abstain_entry_debit_gt_4",
            "selected_method": "mlgbp64",
            "confidence_pct": "",
        },
        {
            "entry_date": "2025-01-10",
            "symbol": "BBB",
            "prediction": "abstain",
            "policy_label": module.BEST_COMBINED_POLICY_LABEL,
            "entry_debit": 1.0,
            "pnl": -0.5,
            "position_size_weight": 1.0,
            "position_sizing_rule": "",
            "selected_method": "median25trend",
            "confidence_pct": "",
        },
        {
            "entry_date": "2025-01-10",
            "symbol": "CCC",
            "prediction": "up",
            "policy_label": module.BEST_COMBINED_POLICY_LABEL,
            "entry_debit": 1.5,
            "pnl": -0.75,
            "position_size_weight": 1.0,
            "position_sizing_rule": "",
            "selected_method": "mllogreg56",
            "confidence_pct": 95.0,
        },
    ]

    filtered = module._derive_skip_filtered_policy_rows(
        rows=rows,
        source_policy_label=module.BEST_COMBINED_POLICY_LABEL,
        derived_policy_label="derived",
        skip_trade_predicates=(
            module._is_abstain_median25trend_trade,
            module._is_high_confidence_up_mllogreg56_trade,
        ),
    )

    assert len(filtered) == 1
    remaining = filtered[0]
    assert remaining["symbol"] == "AAA"
    assert remaining["policy_label"] == "derived"
    assert remaining["position_size_weight"] == 0.5
    assert remaining["position_sizing_rule"] == "half_size_abstain_entry_debit_gt_4"
    assert remaining["entry_debit"] == 2.5
    assert remaining["pnl"] == -1.0
