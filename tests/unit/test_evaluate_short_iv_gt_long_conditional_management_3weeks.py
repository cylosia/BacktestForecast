from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import evaluate_short_iv_gt_long_conditional_management_3weeks as module  # noqa: E402


def test_best_combined_policy_label_points_to_preferred_filtered_variant() -> None:
    assert (
        module.BEST_COMBINED_POLICY_LABEL
        == module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL
    )
    assert (
        module.BEST_COMBINED_PORTFOLIO_POLICY_LABEL
        == module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_POLICY_LABEL
    )
    assert (
        module.BEST_COMBINED_PORTFOLIO_LIVE_POLICY_LABEL
        == f"{module.BEST_COMBINED_PORTFOLIO_POLICY_LABEL}_live"
    )


def test_best_combined_label_chain_derives_from_promoted_base_policy() -> None:
    assert (
        module.BASE_BEST_COMBINED_SYMBOL_SIDE_LOOKBACK_FILTER_POLICY_LABEL
        == f"{module.BASE_BEST_COMBINED_POLICY_LABEL}__symbol_side_52w_lookback_pnl_nonnegative"
    )
    assert (
        module.BEST_COMBINED_TARGETED_UP_SKIP_POLICY_LABEL
        == f"{module.BASE_BEST_COMBINED_POLICY_LABEL}__up_70_75_negative_method_skip"
    )
    assert (
        module.BEST_COMBINED_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL
        == f"{module.BEST_COMBINED_TARGETED_UP_SKIP_POLICY_LABEL}__abstain_debit_gt_4_half_size"
    )


def test_method_side_candidate_label_chain_derives_from_promoted_base_policy() -> None:
    assert (
        module.BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL
        == f"{module.BASE_BEST_COMBINED_POLICY_LABEL}__abstain_mlgbp64_tp0_stop50__abstain_mlgbp72_tp0_stop65"
    )
    assert (
        module.BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_POLICY_LABEL
        == f"{module.BASE_BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__up_70_75_negative_method_skip"
    )
    assert (
        module.BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_ABSTAIN_HALF_SIZE_POLICY_LABEL
        == f"{module.BEST_COMBINED_METHOD_SIDE_EXIT_TARGETED_UP_SKIP_POLICY_LABEL}__abstain_debit_gt_4_half_size"
    )
    assert (
        module.BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_ABSTAIN_FILTER_POLICY_LABEL
        == f"{module.BEST_COMBINED_METHOD_SIDE_EXIT_DEBIT_SENSITIVE_UP_FILTER_POLICY_LABEL}__skip_abstain_debit_sensitive_methods"
    )
    assert (
        module.BEST_COMBINED_TOP18_SYMBOL_MEDIAN_ROI_MIN1_POLICY_LABEL
        == f"{module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top18_52w_symbol_median_roi_min1"
    )
    assert (
        module.BEST_COMBINED_TOP18_SYMBOL_MEDIAN_ROI_MIN3_POLICY_LABEL
        == f"{module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top18_52w_symbol_median_roi_min3"
    )
    assert (
        module.BEST_COMBINED_TOP40_SYMBOL_MEDIAN_ROI_MIN3_POLICY_LABEL
        == f"{module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top40_52w_symbol_median_roi_min3"
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN1_POLICY_LABEL
        == f"{module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top43_52w_symbol_median_roi_min1"
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_POLICY_LABEL
        == f"{module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top43_52w_symbol_median_roi_min3"
    )
    assert (
        module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL
        == f"{module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__skip_vote40rsi_mlgbp68"
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_POLICY_LABEL
        == f"{module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_median_roi_min3"
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_ABSTAIN_CAP29_POLICY_LABEL
        == f"{module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top43_52w_symbol_median_roi_min3__abstain_cap29"
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MINUS_NEGATIVE_P25_MIN3_POLICY_LABEL
        == f"{module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL}__top43_52w_symbol_median_roi_minus_negative_p25_min3"
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


def test_select_abstain_method_side_exit_row_uses_targeted_overrides_only() -> None:
    default_row = {"policy": "default"}
    mlgbp64_row = {"policy": "mlgbp64"}
    mlgbp72_row = {"policy": "mlgbp72"}
    overrides = {
        "mlgbp64": mlgbp64_row,
        "mlgbp72": mlgbp72_row,
    }

    assert (
        module._select_abstain_method_side_exit_row(
            prediction="abstain",
            selected_method="mlgbp64",
            default_row=default_row,
            override_rows_by_method=overrides,
        )
        is mlgbp64_row
    )
    assert (
        module._select_abstain_method_side_exit_row(
            prediction="abstain",
            selected_method="mlgbp72",
            default_row=default_row,
            override_rows_by_method=overrides,
        )
        is mlgbp72_row
    )
    assert (
        module._select_abstain_method_side_exit_row(
            prediction="abstain",
            selected_method="mlgbp68",
            default_row=default_row,
            override_rows_by_method=overrides,
        )
        is default_row
    )
    assert (
        module._select_abstain_method_side_exit_row(
            prediction="up",
            selected_method="mlgbp64",
            default_row=default_row,
            override_rows_by_method=overrides,
        )
        is default_row
    )


def test_resolve_method_side_tp_stop_uses_prediction_and_method_overrides() -> None:
    assert module.resolve_method_side_tp_stop(prediction="abstain", selected_method="mlgbp64") == (0.0, 50.0)
    assert module.resolve_method_side_tp_stop(prediction="abstain", selected_method="mlgbp72") == (0.0, 65.0)
    assert module.resolve_method_side_tp_stop(prediction="abstain", selected_method="median25trend") == (25.0, 35.0)
    assert module.resolve_method_side_tp_stop(prediction="up", selected_method="mlgbp64") == (75.0, 65.0)


def test_is_vix_weekly_change_within_threshold_uses_absolute_change() -> None:
    assert module._is_vix_weekly_change_within_threshold(weekly_change_pct=20.0, threshold_pct=20.0) is True
    assert module._is_vix_weekly_change_within_threshold(weekly_change_pct=-20.0, threshold_pct=20.0) is True
    assert module._is_vix_weekly_change_within_threshold(weekly_change_pct=20.1, threshold_pct=20.0) is False
    assert module._is_vix_weekly_change_within_threshold(weekly_change_pct=-20.1, threshold_pct=20.0) is False
    assert module._is_vix_weekly_change_within_threshold(weekly_change_pct=None, threshold_pct=20.0) is None
    assert module._is_vix_weekly_change_within_threshold(weekly_change_pct=5.0, threshold_pct=None) is None


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


def test_derive_symbol_median_roi_topk_rows_ranks_weekly_candidates_from_full_source_history() -> None:
    rows = [
        {"entry_date": "2025-01-03", "symbol": "AAA", "prediction": "up", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.2, "roi_pct": 10.0},
        {"entry_date": "2025-01-03", "symbol": "BBB", "prediction": "up", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.4, "roi_pct": 20.0},
        {"entry_date": "2025-01-03", "symbol": "CCC", "prediction": "up", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.6, "roi_pct": 30.0},
        {"entry_date": "2025-01-10", "symbol": "AAA", "prediction": "up", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "BBB", "prediction": "up", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "CCC", "prediction": "up", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
    ]

    filtered = module._derive_symbol_median_roi_topk_rows(
        rows=rows,
        source_policy_label="base",
        derived_policy_label="filtered",
        top_k=2,
        min_history_trades=1,
    )

    assert [(row["entry_date"], row["symbol"]) for row in filtered] == [
        ("2025-01-03", "AAA"),
        ("2025-01-03", "BBB"),
        ("2025-01-10", "CCC"),
        ("2025-01-10", "BBB"),
    ]
    assert all(row["policy_label"] == "filtered" for row in filtered)


def test_derive_symbol_median_roi_topk_rows_honors_prediction_caps() -> None:
    rows = [
        {"entry_date": "2025-01-03", "symbol": "AAA", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.8, "roi_pct": 40.0},
        {"entry_date": "2025-01-03", "symbol": "BBB", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.6, "roi_pct": 30.0},
        {"entry_date": "2025-01-03", "symbol": "CCC", "prediction": "up", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.5, "roi_pct": 25.0},
        {"entry_date": "2025-01-03", "symbol": "DDD", "prediction": "up", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.4, "roi_pct": 20.0},
        {"entry_date": "2025-01-10", "symbol": "AAA", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "BBB", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "CCC", "prediction": "up", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "DDD", "prediction": "up", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
    ]

    filtered = module._derive_symbol_median_roi_topk_rows(
        rows=rows,
        source_policy_label="base",
        derived_policy_label="filtered",
        top_k=3,
        min_history_trades=1,
        prediction_caps={"abstain": 1},
    )

    assert [(row["entry_date"], row["symbol"], row["prediction"]) for row in filtered] == [
        ("2025-01-03", "AAA", "abstain"),
        ("2025-01-03", "CCC", "up"),
        ("2025-01-03", "DDD", "up"),
        ("2025-01-10", "AAA", "abstain"),
        ("2025-01-10", "CCC", "up"),
        ("2025-01-10", "DDD", "up"),
    ]


def test_score_history_by_median_roi_minus_negative_p25_penalizes_negative_tail() -> None:
    assert module._score_history_by_median_roi_minus_negative_p25([10.0, 20.0, 30.0, 40.0]) == 25.0
    assert module._score_history_by_median_roi_minus_negative_p25([-40.0, -40.0, 80.0, 80.0]) == -20.0


def test_is_worst_method_trade_targets_vote40rsi_and_mlgbp68_only() -> None:
    assert module._is_worst_method_trade({"selected_method": "vote40rsi"}) is True
    assert module._is_worst_method_trade({"selected_method": "mlgbp68"}) is True
    assert module._is_worst_method_trade({"selected_method": "mlgb72"}) is False


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
            "policy_label": module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
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
            "policy_label": module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
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
            "policy_label": module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
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
        source_policy_label=module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
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


def test_is_debit_sensitive_up_method_trade_uses_method_specific_thresholds() -> None:
    assert module._is_debit_sensitive_up_method_trade(
        {
            "prediction": "up",
            "selected_method": "median25",
            "entry_debit": 3.0,
        }
    ) is True
    assert module._is_debit_sensitive_up_method_trade(
        {
            "prediction": "up",
            "selected_method": "median25",
            "entry_debit": 2.99,
        }
    ) is False
    assert module._is_debit_sensitive_up_method_trade(
        {
            "prediction": "up",
            "selected_method": "mlgb76",
            "entry_debit": 1.2,
        }
    ) is True
    assert module._is_debit_sensitive_up_method_trade(
        {
            "prediction": "up",
            "selected_method": "mlgb76",
            "entry_debit": 1.19,
        }
    ) is False
    assert module._is_debit_sensitive_up_method_trade(
        {
            "prediction": "abstain",
            "selected_method": "median30trend",
            "entry_debit": 2.5,
        }
    ) is False


def test_is_debit_sensitive_abstain_method_trade_uses_method_specific_ranges() -> None:
    assert module._is_debit_sensitive_abstain_method_trade(
        {
            "prediction": "abstain",
            "selected_method": "median40rsi",
            "entry_debit": 1.0,
        }
    ) is True
    assert module._is_debit_sensitive_abstain_method_trade(
        {
            "prediction": "abstain",
            "selected_method": "median40rsi",
            "entry_debit": 1.75,
        }
    ) is False
    assert module._is_debit_sensitive_abstain_method_trade(
        {
            "prediction": "abstain",
            "selected_method": "median40rsi",
            "entry_debit": 2.99,
        }
    ) is True
    assert module._is_debit_sensitive_abstain_method_trade(
        {
            "prediction": "abstain",
            "selected_method": "mlgb70",
            "entry_debit": 1.49,
        }
    ) is True
    assert module._is_debit_sensitive_abstain_method_trade(
        {
            "prediction": "abstain",
            "selected_method": "mlgb72",
            "entry_debit": 0.99,
        }
    ) is True
    assert module._is_debit_sensitive_abstain_method_trade(
        {
            "prediction": "up",
            "selected_method": "median40rsi",
            "entry_debit": 2.5,
        }
    ) is False
