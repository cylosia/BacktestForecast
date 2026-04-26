from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import compare_short_iv_gt_long_management_rules_3weeks as mgmt  # noqa: E402
import evaluate_short_iv_gt_long_conditional_management_3weeks as module  # noqa: E402


def test_best_combined_policy_label_points_to_preferred_filtered_variant() -> None:
    assert (
        module.BEST_COMBINED_POLICY_LABEL
        == module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL
    )
    assert (
        module.BEST_COMBINED_PORTFOLIO_POLICY_LABEL
        == module.BEST_COMBINED_SOURCE_BASKET_CLOSE_70_POLICY_LABEL
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
        module.BEST_COMBINED_EXTENDED_WORST_METHOD_SKIP_POLICY_LABEL
        == f"{module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__skip_mlgb68"
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_EXTENDED_WORST_METHOD_SKIP_POLICY_LABEL
        == f"{module.BEST_COMBINED_EXTENDED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_median_roi_min3"
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL
        == f"{module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_median_roi_min3__method_cap12"
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAPS_10_10_2_POLICY_LABEL
        == (
            f"{module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}"
            "__top43_52w_symbol_median_roi_min3__mlgbp72_cap10__mlgb76_cap10__median40rsi_cap2"
        )
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAPS_9_10_2_POLICY_LABEL
        == (
            f"{module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}"
            "__top43_52w_symbol_median_roi_min3__mlgbp72_cap9__mlgb76_cap10__median40rsi_cap2"
        )
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_P25_NONNEG_POLICY_LABEL
        == (
            f"{module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL}"
            "__p25_nonnegative"
        )
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN5_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL
        == f"{module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_median_roi_min5__method_cap12"
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_PNL_OVER_DEBIT_15_MIN5_POLICY_LABEL
        == (
            f"{module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL}"
            "__pnl_over_debit_15_min5"
        )
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_VIX_ABS_GT_10_HALF_SIZE_POLICY_LABEL
        == (
            f"{module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_POLICY_LABEL}"
            "__vix_abs_weekly_change_gt_10_half_size"
        )
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_STRESS_METHOD_HALF_SIZE_POLICY_LABEL
        == (
            f"{module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL}"
            "__stress_median40rsi_mllogreg56_half_size"
        )
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_WEEKLY_DEBIT_BUDGET40_POLICY_LABEL
        == (
            f"{module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL}"
            "__weekly_debit_budget_40"
        )
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_LOWEST_DRAWDOWN_PCT_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL
        == f"{module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_lowest_drawdown_pct_min3__method_cap12"
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MINUS_DRAWDOWN_PCT_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL
        == (
            f"{module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}"
            "__top43_52w_symbol_median_roi_minus_drawdown_pct_min3__method_cap12"
        )
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_PLUS_P25_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL
        == f"{module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_median_roi_plus_p25_min3__method_cap12"
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_MEDIAN_ROI_MINUS_CVAR10_LOSS_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL
        == (
            f"{module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}"
            "__top43_52w_symbol_median_roi_minus_cvar10_loss_min3__method_cap12"
        )
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_PROFIT_FACTOR_GUARDED_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL
        == f"{module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_profit_factor_guarded_min3__method_cap12"
    )
    assert (
        module.BEST_COMBINED_TOP43_SYMBOL_SORTINO_GUARDED_MIN3_WORST_METHOD_SKIP_METHOD_CAP12_POLICY_LABEL
        == f"{module.BEST_COMBINED_WORST_METHOD_SKIP_POLICY_LABEL}__top43_52w_symbol_sortino_guarded_min3__method_cap12"
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


def test_build_parser_accepts_min_short_over_long_iv_premium_pct() -> None:
    args = module.build_parser().parse_args(["--min-short-over-long-iv-premium-pct", "10"])
    assert args.min_short_over_long_iv_premium_pct == 10.0


def test_entry_atm_iv_metrics_returns_common_atm_iv_premium(monkeypatch) -> None:
    def fake_estimate_call_iv_pct(
        *,
        option_price: float,
        spot_price: float,
        strike_price: float,
        trade_date: date,
        expiration_date: date,
    ) -> float:
        assert spot_price == 100.0
        assert strike_price == 100.0
        return option_price * 10.0

    monkeypatch.setattr(module.tp_grid.delta_grid, "_estimate_call_iv_pct", fake_estimate_call_iv_pct)

    trade_row = {
        "entry_date": "2025-01-03",
        "short_expiration": "2025-01-10",
        "long_expiration": "2025-01-17",
        "spot_close_entry": "100.0",
    }
    option_rows_by_date = {
        date(2025, 1, 3): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow("short99", date(2025, 1, 3), date(2025, 1, 10), 99.0, 4.0),
                module.tp_grid.delta_grid.OptionRow("short100", date(2025, 1, 3), date(2025, 1, 10), 100.0, 5.0),
                module.tp_grid.delta_grid.OptionRow("short101", date(2025, 1, 3), date(2025, 1, 10), 101.0, 4.5),
            ],
            date(2025, 1, 17): [
                module.tp_grid.delta_grid.OptionRow("long100", date(2025, 1, 3), date(2025, 1, 17), 100.0, 4.0),
                module.tp_grid.delta_grid.OptionRow("long101", date(2025, 1, 3), date(2025, 1, 17), 101.0, 3.5),
            ],
        }
    }

    short_iv_pct, long_iv_pct, premium_pct = module._entry_atm_iv_metrics(
        trade_row=trade_row,
        option_rows_by_date=option_rows_by_date,
    )

    assert short_iv_pct == 50.0
    assert long_iv_pct == 40.0
    assert premium_pct == 25.0


def test_with_condition_metadata_marks_iv_premium_threshold_condition() -> None:
    enriched = module._with_condition_metadata(
        {"entry_date": "2025-01-03", "policy_label": "base"},
        short_entry_iv_pct=42.0,
        short_atm_entry_iv_pct=45.0,
        long_atm_entry_iv_pct=40.0,
        short_over_long_atm_iv_premium_pct=12.5,
        vix_snapshot=None,
        vix_max_weekly_change_up_pct=20.0,
        min_short_over_long_iv_premium_pct=10.0,
        condition_debit_gt_1_5=True,
        condition_short_iv_gt_100=False,
        condition_short_iv_gt_110=False,
        condition_short_iv_gt_130=False,
        condition_abstain_debit_gt_5_0_iv_35_50=False,
        condition_abstain_debit_gt_2_0_iv_40_45=False,
        condition_abstain_debit_gt_3_0_iv_55_65=False,
        condition_abstain_debit_gt_2_5_iv_55_80=False,
        condition_abstain_piecewise_moderate_iv=False,
        condition_abstain_midhigh_iv_tested_exit=False,
        condition_up_debit_gt_5_5=False,
        condition_up_short_iv_lt_40=False,
        management_applied=False,
    )

    assert enriched["short_atm_entry_iv_pct"] == 45.0
    assert enriched["long_atm_entry_iv_pct"] == 40.0
    assert enriched["short_over_long_atm_iv_premium_pct"] == 12.5
    assert enriched["condition_short_over_long_atm_iv_premium_ge_threshold"] == 1


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


def test_derive_symbol_median_roi_topk_rows_honors_selected_method_cap() -> None:
    rows = [
        {"entry_date": "2025-01-03", "symbol": "AAA", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.8, "roi_pct": 40.0},
        {"entry_date": "2025-01-03", "symbol": "BBB", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.6, "roi_pct": 30.0},
        {"entry_date": "2025-01-03", "symbol": "CCC", "prediction": "up", "selected_method": "m2", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.5, "roi_pct": 25.0},
        {"entry_date": "2025-01-03", "symbol": "DDD", "prediction": "up", "selected_method": "m3", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.4, "roi_pct": 20.0},
        {"entry_date": "2025-01-10", "symbol": "AAA", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "BBB", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "CCC", "prediction": "up", "selected_method": "m2", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "DDD", "prediction": "up", "selected_method": "m3", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
    ]

    filtered = module._derive_symbol_median_roi_topk_rows(
        rows=rows,
        source_policy_label="base",
        derived_policy_label="filtered",
        top_k=3,
        min_history_trades=1,
        selected_method_cap=1,
    )

    assert [(row["entry_date"], row["symbol"], row["selected_method"]) for row in filtered] == [
        ("2025-01-03", "AAA", "m1"),
        ("2025-01-03", "CCC", "m2"),
        ("2025-01-03", "DDD", "m3"),
        ("2025-01-10", "AAA", "m1"),
        ("2025-01-10", "CCC", "m2"),
        ("2025-01-10", "DDD", "m3"),
    ]


def test_derive_symbol_median_roi_topk_rows_honors_method_specific_caps() -> None:
    rows = [
        {"entry_date": "2025-01-03", "symbol": "AAA", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.8, "roi_pct": 40.0},
        {"entry_date": "2025-01-03", "symbol": "BBB", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.6, "roi_pct": 30.0},
        {"entry_date": "2025-01-03", "symbol": "CCC", "prediction": "up", "selected_method": "m2", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.5, "roi_pct": 25.0},
        {"entry_date": "2025-01-03", "symbol": "DDD", "prediction": "up", "selected_method": "m3", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.4, "roi_pct": 20.0},
        {"entry_date": "2025-01-10", "symbol": "AAA", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "BBB", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "CCC", "prediction": "up", "selected_method": "m2", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "DDD", "prediction": "up", "selected_method": "m3", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
    ]

    filtered = module._derive_symbol_median_roi_topk_rows(
        rows=rows,
        source_policy_label="base",
        derived_policy_label="filtered",
        top_k=3,
        min_history_trades=1,
        selected_method_caps={"m1": 1, "m2": 1},
    )

    assert [(row["entry_date"], row["symbol"], row["selected_method"]) for row in filtered] == [
        ("2025-01-03", "AAA", "m1"),
        ("2025-01-03", "CCC", "m2"),
        ("2025-01-03", "DDD", "m3"),
        ("2025-01-10", "AAA", "m1"),
        ("2025-01-10", "CCC", "m2"),
        ("2025-01-10", "DDD", "m3"),
    ]


def test_derive_symbol_median_roi_topk_rows_honors_history_eligibility_predicate() -> None:
    rows = [
        {"entry_date": "2025-01-03", "symbol": "AAA", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": -0.8, "roi_pct": -40.0},
        {"entry_date": "2025-01-03", "symbol": "BBB", "prediction": "abstain", "selected_method": "m2", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.6, "roi_pct": 30.0},
        {"entry_date": "2025-01-10", "symbol": "AAA", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": -0.2, "roi_pct": -10.0},
        {"entry_date": "2025-01-10", "symbol": "BBB", "prediction": "abstain", "selected_method": "m2", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-17", "symbol": "AAA", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-17", "symbol": "BBB", "prediction": "abstain", "selected_method": "m2", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-24", "symbol": "AAA", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-24", "symbol": "BBB", "prediction": "abstain", "selected_method": "m2", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
    ]

    filtered = module._derive_symbol_median_roi_topk_rows(
        rows=rows,
        source_policy_label="base",
        derived_policy_label="filtered",
        top_k=1,
        min_history_trades=3,
        history_eligibility_predicate=module._has_nonnegative_history_p25,
    )

    assert [(row["entry_date"], row["symbol"]) for row in filtered] == [
        ("2025-01-24", "BBB"),
    ]


def test_derive_symbol_median_roi_topk_rows_honors_weekly_positive_entry_debit_budget() -> None:
    rows = [
        {"entry_date": "2025-01-03", "symbol": "AAA", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.2, "roi_pct": 10.0},
        {"entry_date": "2025-01-03", "symbol": "BBB", "prediction": "abstain", "selected_method": "m2", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.4, "roi_pct": 20.0},
        {"entry_date": "2025-01-03", "symbol": "CCC", "prediction": "abstain", "selected_method": "m3", "policy_label": "base", "entry_debit": 1.2, "pnl": 0.6, "roi_pct": 30.0},
        {"entry_date": "2025-01-10", "symbol": "AAA", "prediction": "abstain", "selected_method": "m1", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "BBB", "prediction": "abstain", "selected_method": "m2", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.1, "roi_pct": 5.0},
        {"entry_date": "2025-01-10", "symbol": "CCC", "prediction": "abstain", "selected_method": "m3", "policy_label": "base", "entry_debit": 1.2, "pnl": 0.1, "roi_pct": 5.0},
    ]

    filtered = module._derive_symbol_median_roi_topk_rows(
        rows=rows,
        source_policy_label="base",
        derived_policy_label="filtered",
        top_k=3,
        min_history_trades=1,
        weekly_positive_entry_debit_budget=3.5,
    )

    assert [(row["entry_date"], row["symbol"]) for row in filtered] == [
        ("2025-01-03", "AAA"),
        ("2025-01-03", "CCC"),
        ("2025-01-10", "CCC"),
        ("2025-01-10", "BBB"),
    ]


def test_derive_symbol_lookback_pnl_over_debit_filtered_rows_uses_source_history() -> None:
    rows = [
        {"entry_date": "2025-01-03", "symbol": "AAA", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.0, "roi_pct": 0.0},
        {"entry_date": "2025-01-10", "symbol": "AAA", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.0, "roi_pct": 0.0},
        {"entry_date": "2025-01-17", "symbol": "AAA", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.0, "roi_pct": 0.0},
        {"entry_date": "2025-01-24", "symbol": "AAA", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.0, "roi_pct": 0.0},
        {"entry_date": "2025-01-31", "symbol": "AAA", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.0, "roi_pct": 0.0},
        {"entry_date": "2025-02-07", "symbol": "AAA", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 4.0, "roi_pct": 200.0},
        {"entry_date": "2025-02-14", "symbol": "AAA", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.2, "roi_pct": 10.0},
        {"entry_date": "2025-01-03", "symbol": "BBB", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 1.0, "roi_pct": 50.0},
        {"entry_date": "2025-01-10", "symbol": "BBB", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 1.0, "roi_pct": 50.0},
        {"entry_date": "2025-01-17", "symbol": "BBB", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 1.0, "roi_pct": 50.0},
        {"entry_date": "2025-01-24", "symbol": "BBB", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 1.0, "roi_pct": 50.0},
        {"entry_date": "2025-01-31", "symbol": "BBB", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 1.0, "roi_pct": 50.0},
        {"entry_date": "2025-02-07", "symbol": "BBB", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.2, "roi_pct": 10.0},
        {"entry_date": "2025-02-14", "symbol": "BBB", "prediction": "abstain", "policy_label": "base", "entry_debit": 2.0, "pnl": 0.2, "roi_pct": 10.0},
    ]

    filtered = module._derive_symbol_lookback_pnl_over_debit_filtered_rows(
        rows=rows,
        source_policy_label="base",
        derived_policy_label="filtered",
        min_history_trades=5,
        min_pnl_over_debit_pct=15.0,
    )

    assert [(row["entry_date"], row["symbol"]) for row in filtered] == [
        ("2025-01-03", "AAA"),
        ("2025-01-03", "BBB"),
        ("2025-01-10", "AAA"),
        ("2025-01-10", "BBB"),
        ("2025-01-17", "AAA"),
        ("2025-01-17", "BBB"),
        ("2025-01-24", "AAA"),
        ("2025-01-24", "BBB"),
        ("2025-01-31", "AAA"),
        ("2025-01-31", "BBB"),
        ("2025-02-07", "BBB"),
        ("2025-02-14", "AAA"),
        ("2025-02-14", "BBB"),
    ]


def test_score_history_by_median_roi_minus_negative_p25_penalizes_negative_tail() -> None:
    assert module._score_history_by_median_roi_minus_negative_p25([10.0, 20.0, 30.0, 40.0]) == 25.0
    assert module._score_history_by_median_roi_minus_negative_p25([-40.0, -40.0, 80.0, 80.0]) == -20.0


def test_history_max_drawdown_pct_uses_chronological_roi_curve() -> None:
    assert round(module._history_max_drawdown_pct([10.0, -20.0, 5.0]) or 0.0, 4) == 18.1818
    assert round(module._history_max_drawdown_pct([-40.0, 80.0, -20.0]) or 0.0, 4) == 40.0


def test_score_history_by_lowest_drawdown_pct_prefers_smaller_drawdown() -> None:
    assert round(module._score_history_by_lowest_drawdown_pct([10.0, -20.0, 5.0]) or 0.0, 4) == -18.1818
    assert round(module._score_history_by_lowest_drawdown_pct([-40.0, 80.0, -20.0]) or 0.0, 4) == -40.0


def test_score_history_by_median_roi_minus_drawdown_pct_penalizes_deeper_history_drawdown() -> None:
    assert round(module._score_history_by_median_roi_minus_drawdown_pct([10.0, -20.0, 5.0]) or 0.0, 4) == -13.1818
    assert round(module._score_history_by_median_roi_minus_drawdown_pct([-40.0, 80.0, -20.0]) or 0.0, 4) == -60.0


def test_score_history_by_median_roi_plus_p25_rewards_stronger_lower_quartile() -> None:
    assert round(module._score_history_by_median_roi_plus_p25([10.0, 20.0, 30.0]) or 0.0, 4) == 35.0
    assert round(module._score_history_by_median_roi_plus_p25([-40.0, 10.0, 30.0, 50.0]) or 0.0, 4) == 17.5


def test_history_cvar_loss_pct_measures_worst_tail_loss_only() -> None:
    assert round(module._history_cvar_loss_pct([10.0, 20.0, 30.0]) or 0.0, 4) == 0.0
    assert round(module._history_cvar_loss_pct([-40.0, 10.0, 30.0, 50.0]) or 0.0, 4) == 40.0


def test_score_history_by_median_roi_minus_cvar10_loss_penalizes_tail_loss() -> None:
    assert round(module._score_history_by_median_roi_minus_cvar10_loss([10.0, 20.0, 30.0]) or 0.0, 4) == 20.0
    assert round(module._score_history_by_median_roi_minus_cvar10_loss([-40.0, 10.0, 30.0, 50.0]) or 0.0, 4) == -20.0


def test_score_history_by_profit_factor_guarded_caps_explosive_win_only_histories() -> None:
    assert round(module._score_history_by_profit_factor_guarded([10.0, 20.0, -5.0]) or 0.0, 4) == 19.4591
    assert round(module._score_history_by_profit_factor_guarded([10.0, 20.0, 30.0]) or 0.0, 4) == 47.9579


def test_score_history_by_sortino_guarded_caps_zero_downside_and_penalizes_losses() -> None:
    assert round(module._score_history_by_sortino_guarded([10.0, 20.0, 30.0]) or 0.0, 4) == 5.0
    assert round(module._score_history_by_sortino_guarded([10.0, 20.0, -10.0]) or 0.0, 4) == 1.1547


def test_is_worst_method_trade_targets_vote40rsi_and_mlgbp68_only() -> None:
    assert module._is_worst_method_trade({"selected_method": "vote40rsi"}) is True
    assert module._is_worst_method_trade({"selected_method": "mlgbp68"}) is True
    assert module._is_worst_method_trade({"selected_method": "mlgb72"}) is False


def test_is_extended_worst_method_trade_also_targets_mlgb68() -> None:
    assert module._is_extended_worst_method_trade({"selected_method": "vote40rsi"}) is True
    assert module._is_extended_worst_method_trade({"selected_method": "mlgbp68"}) is True
    assert module._is_extended_worst_method_trade({"selected_method": "mlgb68"}) is True
    assert module._is_extended_worst_method_trade({"selected_method": "mlgb72"}) is False


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


def test_derive_soft_vix_half_size_policy_rows_compounds_existing_position_size() -> None:
    rows = [
        {
            "entry_date": "2025-01-10",
            "symbol": "AAA",
            "prediction": "abstain",
            "policy_label": "base",
            "entry_debit": 2.5,
            "original_entry_debit": 3.0,
            "spread_mark": 0.75,
            "pnl": -1.0,
            "roll_net_debit": 0.2,
            "roi_pct": -40.0,
            "vix_weekly_change_pct": 12.0,
            "position_size_weight": 0.5,
            "position_sizing_rule": "half_size_abstain_entry_debit_gt_4",
        },
        {
            "entry_date": "2025-01-17",
            "symbol": "BBB",
            "prediction": "abstain",
            "policy_label": "base",
            "entry_debit": 1.0,
            "original_entry_debit": 1.0,
            "spread_mark": 0.25,
            "pnl": 0.2,
            "roll_net_debit": 0.0,
            "roi_pct": 20.0,
            "vix_weekly_change_pct": 8.0,
            "position_size_weight": 1.0,
            "position_sizing_rule": "",
        },
    ]

    filtered = module._derive_soft_vix_half_size_policy_rows(
        rows=rows,
        source_policy_label="base",
        derived_policy_label="derived",
        abs_vix_weekly_change_threshold_pct=10.0,
    )

    assert len(filtered) == 2
    half_sized = filtered[0]
    assert half_sized["policy_label"] == "derived"
    assert half_sized["position_size_weight"] == 0.25
    assert half_sized["position_sizing_rule"] == "half_size_abstain_entry_debit_gt_4|half_size_vix_abs_weekly_change_gt_10"
    assert half_sized["entry_debit"] == 1.25
    assert half_sized["original_entry_debit"] == 1.5
    assert half_sized["spread_mark"] == 0.375
    assert half_sized["pnl"] == -0.5
    assert half_sized["roll_net_debit"] == 0.1
    unchanged = filtered[1]
    assert unchanged["position_size_weight"] == 1.0
    assert unchanged["position_sizing_rule"] == ""
    assert unchanged["entry_debit"] == 1.0


def test_derive_stress_method_half_size_policy_rows_only_half_sizes_targeted_methods() -> None:
    rows = [
        {
            "entry_date": "2025-01-10",
            "symbol": "AAA",
            "prediction": "abstain",
            "policy_label": "base",
            "entry_debit": 2.5,
            "original_entry_debit": 2.5,
            "spread_mark": 0.75,
            "pnl": -1.0,
            "roll_net_debit": 0.2,
            "roi_pct": -40.0,
            "vix_weekly_change_pct": 12.0,
            "position_size_weight": 1.0,
            "position_sizing_rule": "",
            "selected_method": "median40rsi",
        },
        {
            "entry_date": "2025-01-10",
            "symbol": "BBB",
            "prediction": "up",
            "policy_label": "base",
            "entry_debit": 1.5,
            "original_entry_debit": 1.5,
            "spread_mark": 0.4,
            "pnl": 0.3,
            "roll_net_debit": 0.0,
            "roi_pct": 20.0,
            "vix_weekly_change_pct": 12.0,
            "position_size_weight": 1.0,
            "position_sizing_rule": "",
            "selected_method": "median25trend",
        },
    ]

    filtered = module._derive_stress_method_half_size_policy_rows(
        rows=rows,
        source_policy_label="base",
        derived_policy_label="derived",
        stressed_methods=module.STRESS_HALF_SIZE_METHODS,
        abs_vix_weekly_change_threshold_pct=10.0,
    )

    assert len(filtered) == 2
    half_sized = filtered[0]
    assert half_sized["position_size_weight"] == 0.5
    assert half_sized["position_sizing_rule"] == "half_size_stress_methods_vix_abs_weekly_change_gt_10"
    assert half_sized["entry_debit"] == 1.25
    assert half_sized["pnl"] == -0.5
    unchanged = filtered[1]
    assert unchanged["position_size_weight"] == 1.0
    assert unchanged["position_sizing_rule"] == ""
    assert unchanged["entry_debit"] == 1.5


def test_simulate_exit_last_pre_expiration_if_negative_exits_on_last_pre_expiration_mark() -> None:
    trade_row = {
        "symbol": "ABC",
        "entry_date": "2025-01-03",
        "prediction": "abstain",
        "selected_method": "mlgbp72",
        "prediction_engine": "auto",
        "confidence_pct": "",
        "best_delta_target_pct": "50",
        "spot_close_entry": "100",
        "entry_debit": "2.0",
        "short_expiration": "2025-01-10",
        "long_expiration": "2025-01-17",
        "short_strike": "100",
        "spread_mark": "0.25",
        "pnl": "-1.75",
        "short_mark_method": "entry",
        "long_mark_method": "entry",
    }
    option_rows_by_date = {
        date(2025, 1, 9): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00100000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=100.0,
                    close_price=2.5,
                )
            ],
            date(2025, 1, 17): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00100000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 17),
                    strike_price=100.0,
                    close_price=1.0,
                )
            ],
        },
        date(2025, 1, 10): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00100000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 10),
                    strike_price=100.0,
                    close_price=0.0,
                )
            ],
            date(2025, 1, 17): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00100000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 17),
                    strike_price=100.0,
                    close_price=0.5,
                )
            ],
        },
    }
    result = mgmt._simulate_exit_last_pre_expiration_if_negative(
        trade_row=trade_row,
        option_rows_by_date=option_rows_by_date,
        spot_by_date={
            date(2025, 1, 9): 99.0,
            date(2025, 1, 10): 98.0,
        },
        path_dates=[date(2025, 1, 9), date(2025, 1, 10)],
    )

    assert result["exit_date"] == "2025-01-09"
    assert result["exit_reason"] == "last_pre_expiration_negative"
    assert result["spread_mark"] == -1.5
    assert result["pnl"] == -3.5
    assert result["roi_pct"] == -175.0


def test_simulate_abstain_roll_short_forward_one_week_on_first_breach_rolls_to_higher_long_exp_strike() -> None:
    trade_row = {
        "symbol": "ABC",
        "entry_date": "2025-01-03",
        "prediction": "abstain",
        "selected_method": "mlgbp72",
        "prediction_engine": "auto",
        "confidence_pct": "",
        "best_delta_target_pct": "50",
        "spot_close_entry": "100",
        "entry_debit": "1.0",
        "short_expiration": "2025-01-10",
        "long_expiration": "2025-01-17",
        "short_strike": "100",
        "spread_mark": "0.25",
        "pnl": "-0.75",
        "short_mark_method": "entry",
        "long_mark_method": "entry",
    }
    option_rows_by_date = {
        date(2025, 1, 9): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00100000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=100.0,
                    close_price=1.8,
                )
            ],
            date(2025, 1, 17): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00100000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 17),
                    strike_price=100.0,
                    close_price=2.4,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00101000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 17),
                    strike_price=101.0,
                    close_price=1.0,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00102000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 17),
                    strike_price=102.0,
                    close_price=0.8,
                ),
            ],
        },
        date(2025, 1, 17): {
            date(2025, 1, 17): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00100000",
                    trade_date=date(2025, 1, 17),
                    expiration_date=date(2025, 1, 17),
                    strike_price=100.0,
                    close_price=1.2,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00101000",
                    trade_date=date(2025, 1, 17),
                    expiration_date=date(2025, 1, 17),
                    strike_price=101.0,
                    close_price=0.6,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00102000",
                    trade_date=date(2025, 1, 17),
                    expiration_date=date(2025, 1, 17),
                    strike_price=102.0,
                    close_price=0.3,
                ),
            ]
        },
    }

    result = mgmt._simulate_abstain_roll_short_forward_one_week_on_first_breach(
        trade_row=trade_row,
        option_rows_by_date=option_rows_by_date,
        spot_by_date={
            date(2025, 1, 9): 101.5,
            date(2025, 1, 17): 100.5,
        },
        path_dates=[date(2025, 1, 9), date(2025, 1, 17)],
        strike_steps=1,
    )

    assert result["roll_count"] == 1
    assert result["roll_date"] == "2025-01-09"
    assert result["roll_from_strike"] == 100.0
    assert result["roll_to_strike"] == 101.0
    assert result["roll_net_debit"] == 0.8
    assert result["entry_debit"] == 1.8
    assert result["exit_date"] == "2025-01-17"
    assert result["short_strike"] == 101.0


def test_simulate_abstain_roll_short_same_week_atm_once_rolls_to_same_exp_atm_strike() -> None:
    trade_row = {
        "symbol": "ABC",
        "entry_date": "2025-01-03",
        "prediction": "abstain",
        "selected_method": "mlgbp72",
        "prediction_engine": "auto",
        "confidence_pct": "",
        "best_delta_target_pct": "50",
        "spot_close_entry": "100",
        "entry_debit": "1.0",
        "short_expiration": "2025-01-10",
        "long_expiration": "2025-01-17",
        "short_strike": "100",
        "spread_mark": "0.25",
        "pnl": "-0.75",
        "short_mark_method": "entry",
        "long_mark_method": "entry",
    }
    option_rows_by_date = {
        date(2025, 1, 9): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00100000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=100.0,
                    close_price=1.8,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00101000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=101.0,
                    close_price=1.0,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00102000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=102.0,
                    close_price=0.7,
                ),
            ],
            date(2025, 1, 17): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00100000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 17),
                    strike_price=100.0,
                    close_price=2.4,
                )
            ],
        },
        date(2025, 1, 10): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00101000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 10),
                    strike_price=101.0,
                    close_price=0.3,
                )
            ],
            date(2025, 1, 17): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00100000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 17),
                    strike_price=100.0,
                    close_price=0.9,
                )
            ],
        },
    }

    result = mgmt._simulate_abstain_roll_short_same_week_atm_once(
        trade_row=trade_row,
        option_rows_by_date=option_rows_by_date,
        spot_by_date={
            date(2025, 1, 9): 101.5,
            date(2025, 1, 10): 100.2,
        },
        path_dates=[date(2025, 1, 9), date(2025, 1, 10)],
    )

    assert result["roll_count"] == 1
    assert result["roll_date"] == "2025-01-09"
    assert result["roll_from_strike"] == 100.0
    assert result["roll_to_strike"] == 102.0
    assert result["roll_net_debit"] == 1.1
    assert result["entry_debit"] == 2.1
    assert result["exit_date"] == "2025-01-10"
    assert result["short_strike"] == 102.0


def test_simulate_abstain_roll_both_legs_same_week_atm_once_rolls_both_legs_to_common_strike() -> None:
    trade_row = {
        "symbol": "ABC",
        "entry_date": "2025-01-03",
        "prediction": "abstain",
        "selected_method": "mlgbp72",
        "prediction_engine": "auto",
        "confidence_pct": "",
        "best_delta_target_pct": "50",
        "spot_close_entry": "100",
        "entry_debit": "1.0",
        "short_expiration": "2025-01-10",
        "long_expiration": "2025-01-17",
        "short_strike": "100",
        "spread_mark": "0.25",
        "pnl": "-0.75",
        "short_mark_method": "entry",
        "long_mark_method": "entry",
    }
    option_rows_by_date = {
        date(2025, 1, 9): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00100000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=100.0,
                    close_price=1.8,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00102000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=102.0,
                    close_price=0.7,
                ),
            ],
            date(2025, 1, 17): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00100000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 17),
                    strike_price=100.0,
                    close_price=2.4,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00102000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 17),
                    strike_price=102.0,
                    close_price=1.5,
                ),
            ],
        },
        date(2025, 1, 10): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00102000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 10),
                    strike_price=102.0,
                    close_price=0.3,
                )
            ],
            date(2025, 1, 17): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00102000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 17),
                    strike_price=102.0,
                    close_price=0.9,
                )
            ],
        },
    }

    result = mgmt._simulate_abstain_roll_both_legs_same_week_atm_once(
        trade_row=trade_row,
        option_rows_by_date=option_rows_by_date,
        spot_by_date={
            date(2025, 1, 9): 101.5,
            date(2025, 1, 10): 100.2,
        },
        path_dates=[date(2025, 1, 9), date(2025, 1, 10)],
    )

    assert result["roll_count"] == 1
    assert result["roll_date"] == "2025-01-09"
    assert result["roll_from_strike"] == 100.0
    assert result["roll_to_strike"] == 102.0
    assert result["roll_net_debit"] == 0.2
    assert result["entry_debit"] == 1.2
    assert result["exit_date"] == "2025-01-10"
    assert result["short_strike"] == 102.0
    assert result["long_strike"] == 102.0


def test_simulate_abstain_convert_to_same_week_atm_butterfly_on_first_up_breach() -> None:
    trade_row = {
        "symbol": "ABC",
        "entry_date": "2025-01-03",
        "prediction": "abstain",
        "selected_method": "mlgbp72",
        "prediction_engine": "auto",
        "confidence_pct": "",
        "best_delta_target_pct": "50",
        "spot_close_entry": "100",
        "entry_debit": "1.0",
        "short_expiration": "2025-01-10",
        "long_expiration": "2025-01-17",
        "short_strike": "100",
        "long_strike": "100",
        "spread_mark": "0.25",
        "pnl": "-0.75",
        "short_mark_method": "entry",
        "long_mark_method": "entry",
    }
    call_option_rows_by_date = {
        date(2025, 1, 9): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00100000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=100.0,
                    close_price=1.8,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00101000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=101.0,
                    close_price=1.0,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00102000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=102.0,
                    close_price=0.7,
                ),
            ],
            date(2025, 1, 17): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00100000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 17),
                    strike_price=100.0,
                    close_price=2.4,
                )
            ],
        },
        date(2025, 1, 10): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00100000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 10),
                    strike_price=100.0,
                    close_price=0.0,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00101000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 10),
                    strike_price=101.0,
                    close_price=0.3,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00102000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 10),
                    strike_price=102.0,
                    close_price=0.1,
                ),
            ]
        },
    }

    result = mgmt._simulate_abstain_convert_to_same_week_atm_butterfly_on_first_breach(
        trade_row=trade_row,
        call_option_rows_by_date=call_option_rows_by_date,
        put_option_rows_by_date={},
        spot_by_date={
            date(2025, 1, 9): 101.5,
            date(2025, 1, 10): 100.2,
        },
        path_dates=[date(2025, 1, 9), date(2025, 1, 10)],
    )

    assert result["roll_count"] == 1
    assert result["roll_date"] == "2025-01-09"
    assert result["roll_from_strike"] == 100.0
    assert result["roll_to_strike"] == 101.0
    assert result["entry_debit"] == 0.9
    assert result["exit_date"] == "2025-01-10"
    assert result["short_strike"] == 101.0
    assert result["long_strike"] == 101.0


def test_simulate_abstain_convert_to_same_week_atm_butterfly_on_first_up_breach_w2() -> None:
    trade_row = {
        "symbol": "ABC",
        "entry_date": "2025-01-03",
        "prediction": "abstain",
        "selected_method": "mlgbp72",
        "prediction_engine": "auto",
        "confidence_pct": "",
        "best_delta_target_pct": "50",
        "spot_close_entry": "100",
        "entry_debit": "1.0",
        "short_expiration": "2025-01-10",
        "long_expiration": "2025-01-17",
        "short_strike": "100",
        "long_strike": "100",
        "spread_mark": "0.25",
        "pnl": "-0.75",
        "short_mark_method": "entry",
        "long_mark_method": "entry",
    }
    call_option_rows_by_date = {
        date(2025, 1, 9): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00099000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=99.0,
                    close_price=2.5,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00100000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=100.0,
                    close_price=1.8,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00101000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=101.0,
                    close_price=1.0,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00102000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=102.0,
                    close_price=0.7,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00103000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 10),
                    strike_price=103.0,
                    close_price=0.4,
                ),
            ],
            date(2025, 1, 17): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250117C00100000",
                    trade_date=date(2025, 1, 9),
                    expiration_date=date(2025, 1, 17),
                    strike_price=100.0,
                    close_price=2.4,
                )
            ],
        },
        date(2025, 1, 10): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00099000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 10),
                    strike_price=99.0,
                    close_price=1.2,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00100000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 10),
                    strike_price=100.0,
                    close_price=0.6,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00101000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 10),
                    strike_price=101.0,
                    close_price=0.3,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00102000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 10),
                    strike_price=102.0,
                    close_price=0.1,
                ),
                module.tp_grid.delta_grid.OptionRow(
                    option_ticker="O:ABC250110C00103000",
                    trade_date=date(2025, 1, 10),
                    expiration_date=date(2025, 1, 10),
                    strike_price=103.0,
                    close_price=0.0,
                ),
            ]
        },
    }

    result = mgmt._simulate_abstain_convert_to_same_week_atm_butterfly_on_first_breach(
        trade_row=trade_row,
        call_option_rows_by_date=call_option_rows_by_date,
        put_option_rows_by_date={},
        spot_by_date={
            date(2025, 1, 9): 101.5,
            date(2025, 1, 10): 100.2,
        },
        path_dates=[date(2025, 1, 9), date(2025, 1, 10)],
        wing_steps=2,
    )

    assert result["policy_label"] == "abstain_convert_to_same_week_atm_butterfly_on_first_breach_w2"
    assert result["roll_count"] == 1
    assert result["roll_date"] == "2025-01-09"
    assert result["roll_from_strike"] == 100.0
    assert result["roll_to_strike"] == 101.0
    assert result["entry_debit"] == 1.3
    assert result["exit_date"] == "2025-01-10"
    assert result["short_strike"] == 101.0
    assert result["long_strike"] == 101.0


def test_pick_credit_spread_strikes_by_delta_selects_call_short_and_wing() -> None:
    rows_by_strike = {
        strike: module.tp_grid.delta_grid.OptionRow(
            option_ticker=f"O:ABC250110C{int(strike * 1000):08d}",
            trade_date=date(2025, 1, 9),
            expiration_date=date(2025, 1, 10),
            strike_price=strike,
            close_price=price,
        )
        for strike, price in [
            (99.0, 2.5),
            (100.0, 1.8),
            (101.0, 1.0),
            (102.0, 0.7),
            (103.0, 0.4),
            (104.0, 0.2),
        ]
    }

    picked = mgmt._pick_credit_spread_strikes_by_delta(
        rows_by_strike=rows_by_strike,
        spot_mark=101.5,
        trade_date=date(2025, 1, 9),
        expiration=date(2025, 1, 10),
        contract_type="call",
        target_abs_delta=0.4,
        width_steps=2,
    )

    assert picked == (102.0, 104.0)


def test_pick_credit_spread_strikes_by_delta_selects_put_short_and_wing() -> None:
    rows_by_strike = {
        strike: module.tp_grid.delta_grid.OptionRow(
            option_ticker=f"O:ABC250110P{int(strike * 1000):08d}",
            trade_date=date(2025, 1, 9),
            expiration_date=date(2025, 1, 10),
            strike_price=strike,
            close_price=price,
        )
        for strike, price in [
            (99.0, 0.2),
            (100.0, 0.4),
            (101.0, 0.7),
            (102.0, 1.0),
            (103.0, 1.8),
            (104.0, 2.5),
        ]
    }

    picked = mgmt._pick_credit_spread_strikes_by_delta(
        rows_by_strike=rows_by_strike,
        spot_mark=100.0,
        trade_date=date(2025, 1, 9),
        expiration=date(2025, 1, 10),
        contract_type="put",
        target_abs_delta=0.3,
        width_steps=1,
    )

    assert picked == (100.0, 99.0)


def test_simulate_abstain_convert_to_same_week_credit_spread_on_first_up_breach() -> None:
    trade_row = {
        "symbol": "ABC",
        "entry_date": "2025-01-03",
        "prediction": "abstain",
        "selected_method": "mlgbp72",
        "prediction_engine": "auto",
        "confidence_pct": "",
        "best_delta_target_pct": "50",
        "spot_close_entry": "100",
        "entry_debit": "1.0",
        "short_expiration": "2025-01-10",
        "long_expiration": "2025-01-17",
        "short_strike": "100",
        "long_strike": "100",
        "spread_mark": "0.25",
        "pnl": "-0.75",
        "short_mark_method": "entry",
        "long_mark_method": "entry",
    }
    call_option_rows_by_date = {
        date(2025, 1, 9): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow("x", date(2025, 1, 9), date(2025, 1, 10), 99.0, 2.5),
                module.tp_grid.delta_grid.OptionRow("x", date(2025, 1, 9), date(2025, 1, 10), 100.0, 1.8),
                module.tp_grid.delta_grid.OptionRow("x", date(2025, 1, 9), date(2025, 1, 10), 101.0, 1.0),
                module.tp_grid.delta_grid.OptionRow("x", date(2025, 1, 9), date(2025, 1, 10), 102.0, 0.7),
                module.tp_grid.delta_grid.OptionRow("x", date(2025, 1, 9), date(2025, 1, 10), 103.0, 0.4),
                module.tp_grid.delta_grid.OptionRow("x", date(2025, 1, 9), date(2025, 1, 10), 104.0, 0.2),
            ],
            date(2025, 1, 17): [
                module.tp_grid.delta_grid.OptionRow("y", date(2025, 1, 9), date(2025, 1, 17), 100.0, 2.4),
            ],
        },
        date(2025, 1, 10): {
            date(2025, 1, 10): [
                module.tp_grid.delta_grid.OptionRow("x", date(2025, 1, 10), date(2025, 1, 10), 99.0, 1.3),
                module.tp_grid.delta_grid.OptionRow("x", date(2025, 1, 10), date(2025, 1, 10), 100.0, 0.7),
                module.tp_grid.delta_grid.OptionRow("x", date(2025, 1, 10), date(2025, 1, 10), 101.0, 0.35),
                module.tp_grid.delta_grid.OptionRow("x", date(2025, 1, 10), date(2025, 1, 10), 102.0, 0.15),
                module.tp_grid.delta_grid.OptionRow("x", date(2025, 1, 10), date(2025, 1, 10), 103.0, 0.05),
                module.tp_grid.delta_grid.OptionRow("x", date(2025, 1, 10), date(2025, 1, 10), 104.0, 0.0),
            ]
        },
    }

    result = mgmt._simulate_abstain_convert_to_same_week_credit_spread_on_first_breach(
        trade_row=trade_row,
        call_option_rows_by_date=call_option_rows_by_date,
        put_option_rows_by_date={},
        spot_by_date={
            date(2025, 1, 9): 101.5,
            date(2025, 1, 10): 100.2,
        },
        path_dates=[date(2025, 1, 9), date(2025, 1, 10)],
        target_abs_delta_pct=40,
        width_steps=1,
    )

    assert result["policy_label"] == "abstain_convert_to_same_week_credit_spread_on_first_breach_d40_w1"
    assert result["roll_count"] == 1
    assert result["roll_date"] == "2025-01-09"
    assert result["roll_from_strike"] == 100.0
    assert result["roll_to_strike"] == 102.0
    assert result["entry_debit"] == 1.7
    assert result["short_strike"] == 102.0
    assert result["long_strike"] == 103.0
    assert result["pnl"] == -0.2
    assert result["roi_pct"] == -11.764706


def test_derive_targeted_replacement_policy_rows_preserves_existing_position_size_metadata() -> None:
    rows = [
        {
            "entry_date": "2025-01-10",
            "symbol": "AAA",
            "prediction": "abstain",
            "policy_label": module.BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
            "selected_method": "mlgbp72",
            "best_delta_target_pct": 50,
            "short_expiration": "2025-01-17",
            "long_expiration": "2025-01-24",
            "short_strike": 100.0,
            "long_strike": 100.0,
            "original_entry_debit": 2.0,
            "entry_debit": 1.0,
            "spread_mark": 0.4,
            "pnl": -0.6,
            "roll_net_debit": 0.1,
            "roi_pct": -60.0,
            "position_size_weight": 0.5,
            "position_sizing_rule": "half_size_abstain_entry_debit_gt_4",
        },
        {
            "entry_date": "2025-01-10",
            "symbol": "BBB",
            "prediction": "up",
            "policy_label": module.BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
            "selected_method": "median25trend",
            "best_delta_target_pct": 45,
            "short_expiration": "2025-01-17",
            "long_expiration": "2025-01-24",
            "short_strike": 120.0,
            "long_strike": 120.0,
            "original_entry_debit": 1.0,
            "entry_debit": 1.0,
            "spread_mark": 1.3,
            "pnl": 0.3,
            "roll_net_debit": 0.0,
            "roi_pct": 30.0,
            "position_size_weight": 1.0,
            "position_sizing_rule": "",
        },
        {
            "entry_date": "2025-01-10",
            "symbol": "AAA",
            "prediction": "abstain",
            "policy_label": module.MLGBP72_ABSTAIN_LAST_PRE_EXPIRATION_NEGATIVE_POLICY_LABEL,
            "selected_method": "mlgbp72",
            "best_delta_target_pct": 50,
            "short_expiration": "2025-01-17",
            "long_expiration": "2025-01-24",
            "short_strike": 100.0,
            "long_strike": 100.0,
            "original_entry_debit": 4.0,
            "entry_debit": 2.0,
            "spread_mark": 0.5,
            "pnl": -1.5,
            "roll_net_debit": 0.2,
            "roi_pct": -75.0,
            "position_size_weight": 1.0,
            "position_sizing_rule": "",
        },
    ]

    filtered = module._derive_targeted_replacement_policy_rows(
        rows=rows,
        source_policy_label=module.BEST_COMBINED_PORTFOLIO_POLICY_LABEL,
        derived_policy_label="derived",
        replacement_policy_label=module.MLGBP72_ABSTAIN_LAST_PRE_EXPIRATION_NEGATIVE_POLICY_LABEL,
        replacement_trade_predicate=module._is_abstain_mlgbp72_trade,
    )

    assert len(filtered) == 2
    replaced = filtered[0]
    assert replaced["policy_label"] == "derived"
    assert replaced["position_size_weight"] == 0.5
    assert replaced["position_sizing_rule"] == "half_size_abstain_entry_debit_gt_4"
    assert replaced["original_entry_debit"] == 2.0
    assert replaced["entry_debit"] == 1.0
    assert replaced["spread_mark"] == 0.25
    assert replaced["pnl"] == -0.75
    assert replaced["roll_net_debit"] == 0.1
    assert replaced["roi_pct"] == -75.0
    assert filtered[1]["symbol"] == "BBB"
    assert filtered[1]["entry_debit"] == 1.0


def test_derive_weekly_basket_close_policy_rows_reprices_triggered_week(monkeypatch) -> None:
    rows = [
        {
            "entry_date": "2025-01-10",
            "symbol": "AAA",
            "prediction": "abstain",
            "policy_label": module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            "selected_method": "mlgb76",
            "best_delta_target_pct": 50,
            "short_expiration": "2025-01-17",
            "long_expiration": "2025-01-24",
            "short_strike": 100.0,
            "long_strike": 100.0,
            "entry_debit": 2.0,
            "original_entry_debit": 2.0,
            "spread_mark": 1.0,
            "pnl": -1.0,
            "roi_pct": -50.0,
            "exit_date": "2025-01-17",
            "exit_reason": "expiration",
            "holding_days_calendar": 7,
            "position_size_weight": 1.0,
            "position_sizing_rule": "",
            "short_mark_method": "exact",
            "long_mark_method": "exact",
            "roll_count": 0,
        },
        {
            "entry_date": "2025-01-10",
            "symbol": "BBB",
            "prediction": "abstain",
            "policy_label": module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
            "selected_method": "mlgb76",
            "best_delta_target_pct": 50,
            "short_expiration": "2025-01-17",
            "long_expiration": "2025-01-24",
            "short_strike": 100.0,
            "long_strike": 100.0,
            "entry_debit": 2.0,
            "original_entry_debit": 2.0,
            "spread_mark": 1.0,
            "pnl": -1.0,
            "roi_pct": -50.0,
            "exit_date": "2025-01-17",
            "exit_reason": "expiration",
            "holding_days_calendar": 7,
            "position_size_weight": 1.0,
            "position_sizing_rule": "",
            "short_mark_method": "exact",
            "long_mark_method": "exact",
            "roll_count": 0,
        },
    ]

    def fake_load_symbol_path_cache(session, *, symbol, trades):
        return (
            {date(2025, 1, 13): 100.0},
            {},
            {(trades[0]["entry_date"], symbol, trades[0]["prediction"]): [date(2025, 1, 13)]},
        )

    def fake_mark_position(**kwargs):
        if kwargs["short_strike"] == 100.0:
            return {
                "short_mark": 0.0,
                "long_mark": 3.5,
                "spread_mark": 3.5,
                "short_mark_method": "exact",
                "long_mark_method": "exact",
            }
        return None

    monkeypatch.setattr(module.mgmt, "_load_symbol_path_cache", fake_load_symbol_path_cache)
    monkeypatch.setattr(module.mgmt, "_mark_position", fake_mark_position)

    derived = module._derive_weekly_basket_close_policy_rows(
        rows=rows,
        source_policy_label=module.BEST_COMBINED_METHOD_SIDE_EXIT_POLICY_LABEL,
        derived_policy_label=module.BEST_COMBINED_SOURCE_BASKET_CLOSE_70_POLICY_LABEL,
        threshold_pct=70.0,
        session=None,
    )

    assert len(derived) == 2
    for row in derived:
        assert row["policy_label"] == module.BEST_COMBINED_SOURCE_BASKET_CLOSE_70_POLICY_LABEL
        assert row["exit_date"] == "2025-01-13"
        assert row["exit_reason"] == "basket_close_70"
        assert row["holding_days_calendar"] == 3
        assert row["spread_mark"] == 3.5
        assert row["pnl"] == 1.5
        assert row["roi_pct"] == 75.0


def test_trade_identity_key_uses_roll_from_strike_for_rolled_rows() -> None:
    source_key = module._trade_identity_key(
        {
            "entry_date": "2025-01-10",
            "symbol": "AAA",
            "prediction": "abstain",
            "selected_method": "mlgbp72",
            "best_delta_target_pct": 50,
            "short_expiration": "2025-01-17",
            "long_expiration": "2025-01-24",
            "short_strike": 100.0,
            "long_strike": 100.0,
            "roll_count": 0,
        }
    )
    rolled_key = module._trade_identity_key(
        {
            "entry_date": "2025-01-10",
            "symbol": "AAA",
            "prediction": "abstain",
            "selected_method": "mlgbp72",
            "best_delta_target_pct": 50,
            "short_expiration": "2025-01-17",
            "long_expiration": "2025-01-24",
            "short_strike": 101.0,
            "long_strike": 100.0,
            "roll_count": 1,
            "roll_from_strike": 100.0,
        }
    )

    assert rolled_key == source_key


def test_trade_identity_key_prefers_explicit_source_strikes_for_double_roll_rows() -> None:
    source_key = module._trade_identity_key(
        {
            "entry_date": "2025-01-10",
            "symbol": "AAA",
            "prediction": "abstain",
            "selected_method": "mlgbp72",
            "best_delta_target_pct": 50,
            "short_expiration": "2025-01-17",
            "long_expiration": "2025-01-24",
            "short_strike": 100.0,
            "long_strike": 100.0,
            "roll_count": 0,
        }
    )
    rolled_key = module._trade_identity_key(
        {
            "entry_date": "2025-01-10",
            "symbol": "AAA",
            "prediction": "abstain",
            "selected_method": "mlgbp72",
            "best_delta_target_pct": 50,
            "short_expiration": "2025-01-17",
            "long_expiration": "2025-01-24",
            "short_strike": 102.0,
            "long_strike": 102.0,
            "roll_count": 1,
            "roll_from_strike": 100.0,
            "source_short_strike": 100.0,
            "source_long_strike": 100.0,
        }
    )

    assert rolled_key == source_key


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
