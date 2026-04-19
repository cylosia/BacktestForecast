from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import evaluate_short_iv_gt_long_conditional_management_3weeks as module  # noqa: E402


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
