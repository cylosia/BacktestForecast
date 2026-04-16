from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from grid_search_weekly_calendar_policy_two_stage import (  # noqa: E402
    _build_default_bear_filters,
    _build_default_bull_filters,
    _build_heavy_strategy_sets,
    _build_strategy_sets,
    _classify_trade_regime,
    _expand_strategy_triplet_delta_neighborhood,
    _expand_strategy_triplet_profit_target_neighborhood,
    _parse_profit_target_pcts,
    _unique_strategy_triplets,
)


def test_parse_profit_target_pcts_accepts_csv_and_deduplicates() -> None:
    assert _parse_profit_target_pcts("50, 60,70,75,80,70") == (50, 60, 70, 75, 80)


def test_build_strategy_sets_keep_primary_regimes_on_40_and_50_delta() -> None:
    bullish, bearish, neutral = _build_strategy_sets("AGQ")

    assert len(bullish) == 4
    assert {strategy.delta_target for strategy in bullish} == {40, 50}
    assert len(bearish) == 8
    assert {strategy.delta_target for strategy in bearish} == {40, 50}
    assert len(neutral) == 4


def test_build_heavy_strategy_sets_isolate_30_delta_variants() -> None:
    heavy_bullish, heavy_bearish = _build_heavy_strategy_sets("AGQ")

    assert [strategy.label for strategy in heavy_bullish] == [
        "agq_call_d30_pt50",
        "agq_call_d30_pt75",
    ]
    assert len(heavy_bearish) == 4
    assert {strategy.delta_target for strategy in heavy_bearish} == {30}


def test_refine_profit_target_neighborhood_expands_one_branch_at_a_time() -> None:
    bullish, bearish, neutral = _build_strategy_sets("AGQ")
    seed_triplets = ((bullish[0], bearish[1], neutral[0]),)

    expanded = _expand_strategy_triplet_profit_target_neighborhood(
        seed_triplets,
        (50, 60, 70, 75, 80),
    )

    assert len(expanded) == 13

    bull_profit_targets = {triplet[0].profit_target_pct for triplet in expanded}
    bear_profit_targets = {triplet[1].profit_target_pct for triplet in expanded}
    neutral_profit_targets = {triplet[2].profit_target_pct for triplet in expanded}

    assert bull_profit_targets == {50, 60, 70, 75, 80}
    assert bear_profit_targets == {50, 60, 70, 75, 80}
    assert neutral_profit_targets == {50, 60, 70, 75, 80}

    seed = seed_triplets[0]
    for triplet in expanded:
        changed_branches = sum(
            1
            for index, strategy in enumerate(triplet)
            if strategy.label != seed[index].label
        )
        assert changed_branches <= 1


def test_unique_strategy_triplets_deduplicates_profit_target_variants_by_structure() -> None:
    bullish, bearish, neutral = _build_strategy_sets("AGQ")
    strategy_lookup = {
        strategy.label: strategy
        for strategy in bullish + bearish + neutral
    }
    rows = [
        {
            "bull_strategy": bullish[0].label,
            "bear_strategy": bearish[0].label,
            "neutral_strategy": neutral[0].label,
        },
        {
            "bull_strategy": bullish[1].label,
            "bear_strategy": bearish[1].label,
            "neutral_strategy": neutral[1].label,
        },
        {
            "bull_strategy": bullish[2].label,
            "bear_strategy": bearish[0].label,
            "neutral_strategy": neutral[2].label,
        },
    ]

    selected = _unique_strategy_triplets(rows, strategy_lookup, limit=3)

    assert [
        (bull.label, bear.label, neutral_strategy.label)
        for bull, bear, neutral_strategy in selected
    ] == [
        (bullish[0].label, bearish[0].label, neutral[0].label),
        (bullish[2].label, bearish[0].label, neutral[2].label),
    ]


def test_refine_delta_neighborhood_expands_one_branch_at_a_time() -> None:
    bullish, bearish, neutral = _build_strategy_sets("AGQ")
    seed_triplets = ((bullish[0], bearish[0], neutral[0]),)

    expanded = _expand_strategy_triplet_delta_neighborhood(
        seed_triplets,
        5,
    )

    assert len(expanded) == 10

    bull_deltas = {triplet[0].delta_target for triplet in expanded}
    bear_deltas = {triplet[1].delta_target for triplet in expanded}
    neutral_deltas = {triplet[2].delta_target for triplet in expanded}

    assert bull_deltas == {30, 35, 40, 45}
    assert bear_deltas == {30, 35, 40, 45}
    assert neutral_deltas == {30, 35, 40, 45}

    seed = seed_triplets[0]
    for triplet in expanded:
        changed_branches = sum(
            1
            for index, strategy in enumerate(triplet)
            if strategy.label != seed[index].label
        )
        assert changed_branches <= 1


def test_refine_delta_and_profit_target_expansion_supports_combined_variants() -> None:
    bullish, bearish, neutral = _build_strategy_sets("AGQ")
    seed_triplets = ((bullish[0], bearish[0], neutral[0]),)

    delta_expanded = _expand_strategy_triplet_delta_neighborhood(seed_triplets, 5)
    expanded = _expand_strategy_triplet_profit_target_neighborhood(
        delta_expanded,
        (50, 60, 70, 75, 80),
    )

    assert any(triplet[0].label == "agq_call_d30_pt80" for triplet in expanded)
    assert any(triplet[1].label == "bear_agq_call_d30_pt60" for triplet in expanded)
    assert any(triplet[2].label == "neutral_agq_call_d30_pt70" for triplet in expanded)


def test_classify_trade_regime_distinguishes_heavy_and_regular_branches() -> None:
    bull_filter = _build_default_bull_filters()[0]
    bear_filter = _build_default_bear_filters()[0]

    assert _classify_trade_regime(
        indicator_row={"roc63": 2.0, "adx14": 12.0, "rsi14": 55.0},
        bull_filter=bull_filter,
        bear_filter=bear_filter,
        enable_heavy_bull=False,
        enable_heavy_bear=False,
    ) == "bullish"
    assert _classify_trade_regime(
        indicator_row={"roc63": 8.0, "adx14": 16.0, "rsi14": 70.0},
        bull_filter=bull_filter,
        bear_filter=bear_filter,
        enable_heavy_bull=True,
        enable_heavy_bear=False,
    ) == "heavy_bullish"
    assert _classify_trade_regime(
        indicator_row={"roc63": -2.0, "adx14": 16.0, "rsi14": 35.0},
        bull_filter=bull_filter,
        bear_filter=bear_filter,
        enable_heavy_bull=False,
        enable_heavy_bear=False,
    ) == "bearish"
    assert _classify_trade_regime(
        indicator_row={"roc63": -8.0, "adx14": 25.0, "rsi14": 30.0},
        bull_filter=bull_filter,
        bear_filter=bear_filter,
        enable_heavy_bull=False,
        enable_heavy_bear=True,
    ) == "heavy_bearish"
    assert _classify_trade_regime(
        indicator_row={"roc63": 0.0, "adx14": 5.0, "rsi14": 50.0},
        bull_filter=bull_filter,
        bear_filter=bear_filter,
        enable_heavy_bull=True,
        enable_heavy_bear=True,
    ) == "neutral"
