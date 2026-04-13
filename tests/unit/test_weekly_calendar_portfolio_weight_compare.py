from __future__ import annotations

from scripts.portfolio_weighting import (
    _build_scheme_weights,
    _cap_normalized_weights,
    _normalize_scores,
    _weighted_median,
)


def test_normalize_scores_falls_back_to_equal_weights() -> None:
    weights = _normalize_scores(["A", "B", "C"], {"A": 0.0, "B": -1.0, "C": float("nan")})
    assert weights == {"A": 1 / 3, "B": 1 / 3, "C": 1 / 3}


def test_weighted_median_prefers_halfway_crossing_value() -> None:
    assert _weighted_median([10.0, 20.0, 30.0], [0.2, 0.6, 0.2]) == 20.0
    assert _weighted_median([1.0, 2.0, 3.0, 4.0], [1.0, 1.0, 1.0, 1.0]) == 2.0


def test_build_scheme_weights_normalizes_each_scheme() -> None:
    selection_rows = [
        {
            "rank": "1",
            "symbol": "AAA",
            "training_trade_count": "100",
            "training_median_roi_on_margin_pct": "80",
            "training_total_roi_pct": "8",
        },
        {
            "rank": "2",
            "symbol": "BBB",
            "training_trade_count": "25",
            "training_median_roi_on_margin_pct": "40",
            "training_total_roi_pct": "2",
        },
    ]
    weights_by_scheme = _build_scheme_weights(selection_rows, trade_count_cap=100.0)

    for weights in weights_by_scheme.values():
        assert abs(sum(weights.values()) - 1.0) < 1e-12

    assert weights_by_scheme["median_shrunk"]["AAA"] > weights_by_scheme["median_shrunk"]["BBB"]
    assert weights_by_scheme["total_roi_shrunk"]["AAA"] > weights_by_scheme["total_roi_shrunk"]["BBB"]


def test_cap_normalized_weights_limits_max_weight_and_preserves_total() -> None:
    capped = _cap_normalized_weights({"A": 0.7, "B": 0.2, "C": 0.1}, 0.5)
    assert abs(sum(capped.values()) - 1.0) < 1e-12
    assert capped["A"] == 0.5
    assert capped["B"] > 0.2
    assert capped["C"] > 0.1
