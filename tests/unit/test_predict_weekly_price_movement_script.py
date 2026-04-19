from __future__ import annotations

from datetime import date, timedelta

import pytest

from backtestforecast.market_data.types import DailyBar
from scripts import predict_weekly_price_movement as script


def _make_bars(
    *,
    count: int = 220,
    start_date: date = date(2023, 1, 2),
    start_close: float = 100.0,
    daily_delta: float = 0.35,
    volatility_wave: float = 0.04,
) -> list[DailyBar]:
    bars: list[DailyBar] = []
    close = start_close
    for index in range(count):
        close += daily_delta + (((index % 11) - 5) * volatility_wave)
        range_scale = 0.6 + ((index % 7) * 0.03)
        bars.append(
            DailyBar(
                trade_date=start_date + timedelta(days=index),
                open_price=close - 0.25,
                high_price=close + range_scale,
                low_price=close - range_scale,
                close_price=close,
                volume=1_000_000 + ((index % 9) * 30_000),
            )
        )
    return bars


def _make_regime_bars(
    *,
    count: int = 320,
    start_date: date = date(2023, 1, 2),
    start_close: float = 100.0,
) -> list[DailyBar]:
    bars: list[DailyBar] = []
    close = start_close
    for index in range(count):
        regime = (index // 24) % 4
        daily_delta = 0.8 if regime in (0, 3) else -0.75
        close = max(20.0, close + daily_delta + (((index % 9) - 4) * 0.09))
        range_scale = 0.9 + ((index % 5) * 0.05)
        bars.append(
            DailyBar(
                trade_date=start_date + timedelta(days=index),
                open_price=close - 0.3,
                high_price=close + range_scale,
                low_price=close - range_scale,
                close_price=close,
                volume=1_250_000 + ((index % 13) * 25_000),
            )
        )
    return bars


def _make_option_rows_for_trade_date(
    *,
    trade_date: date,
    spot_price: float,
) -> list[script.OptionFeatureRow]:
    front_expiration = trade_date + timedelta(days=7)
    second_expiration = trade_date + timedelta(days=14)
    atm_strike = round(spot_price)
    otm_call_strike = round(spot_price * 1.05)
    otm_put_strike = round(spot_price * 0.95)
    return [
        script.OptionFeatureRow(
            trade_date=trade_date,
            expiration_date=front_expiration,
            contract_type="call",
            strike_price=float(atm_strike),
            close_price=2.2,
            volume=100.0,
        ),
        script.OptionFeatureRow(
            trade_date=trade_date,
            expiration_date=front_expiration,
            contract_type="put",
            strike_price=float(atm_strike),
            close_price=3.1,
            volume=160.0,
        ),
        script.OptionFeatureRow(
            trade_date=trade_date,
            expiration_date=front_expiration,
            contract_type="call",
            strike_price=float(otm_call_strike),
            close_price=1.2,
            volume=40.0,
        ),
        script.OptionFeatureRow(
            trade_date=trade_date,
            expiration_date=front_expiration,
            contract_type="put",
            strike_price=float(otm_put_strike),
            close_price=1.8,
            volume=55.0,
        ),
        script.OptionFeatureRow(
            trade_date=trade_date,
            expiration_date=second_expiration,
            contract_type="call",
            strike_price=float(atm_strike),
            close_price=3.8,
            volume=90.0,
        ),
        script.OptionFeatureRow(
            trade_date=trade_date,
            expiration_date=second_expiration,
            contract_type="put",
            strike_price=float(atm_strike),
            close_price=4.6,
            volume=110.0,
        ),
    ]


def test_sign_target_matches_requested_formula() -> None:
    assert script._sign_from_future_close(current_close=100.0, future_close=103.0) == 1
    assert script._sign_from_future_close(current_close=100.0, future_close=97.0) == -1
    assert script._sign_from_future_close(current_close=100.0, future_close=100.0) == 0


def test_walk_forward_prediction_rows_use_requested_sign_target() -> None:
    bars = _make_bars(count=260, daily_delta=0.4, volatility_wave=0.02)
    features = script._build_feature_matrix(bars)
    candidates = script._build_analog_candidates(bars=bars, features=features, horizon_bars=5)

    rows = script._walk_forward_predictions(
        bars=bars,
        features=features,
        candidates=candidates,
        start_date=bars[40].trade_date,
        horizon_bars=5,
        min_spacing_bars=5,
        min_candidate_count=20,
        method=script.PredictionMethodConfig(name="median15", vote_mode="median_return", max_analogs=15),
    )

    assert rows
    assert all(int(row["predicted_sign"]) in (-1, 0, 1) for row in rows)
    assert all(int(row["actual_sign"]) in (-1, 0, 1) for row in rows)
    assert all(row["confidence_pct"] is not None for row in rows)
    assert all(row["probability_up_pct"] is not None for row in rows)
    assert all(row["probability_down_pct"] is not None for row in rows)


def test_latest_prediction_tracks_uptrend_series() -> None:
    bars = _make_bars(count=260, daily_delta=0.45, volatility_wave=0.01)
    features = script._build_feature_matrix(bars)
    candidates = script._build_analog_candidates(bars=bars, features=features, horizon_bars=5)

    latest = script._build_latest_prediction(
        bars=bars,
        features=features,
        candidates=candidates,
        horizon_bars=5,
        min_spacing_bars=5,
        min_candidate_count=25,
        method=script.PredictionMethodConfig(name="median20", vote_mode="median_return", max_analogs=20),
    )

    assert latest is not None
    assert latest["predicted_sign"] == 1
    assert float(latest["predicted_return_median_pct"]) > 0
    assert float(latest["confidence_pct"]) >= 0.0
    assert float(latest["probability_up_pct"]) >= 0.0


def test_analog_confidence_threshold_can_abstain_low_consensus_rows() -> None:
    bars = _make_regime_bars()
    features = script._build_feature_matrix(bars)
    candidates = script._build_analog_candidates(bars=bars, features=features, horizon_bars=5)

    unfiltered_rows = script._walk_forward_predictions(
        bars=bars,
        features=features,
        candidates=candidates,
        start_date=bars[80].trade_date,
        horizon_bars=5,
        min_spacing_bars=5,
        min_candidate_count=20,
        method=script.PredictionMethodConfig(name="vote20", vote_mode="weighted_vote", max_analogs=20),
    )
    filtered_rows = script._walk_forward_predictions(
        bars=bars,
        features=features,
        candidates=candidates,
        start_date=bars[80].trade_date,
        horizon_bars=5,
        min_spacing_bars=5,
        min_candidate_count=20,
        method=script.PredictionMethodConfig(
            name="vote20c65",
            vote_mode="weighted_vote",
            max_analogs=20,
            confidence_threshold=0.65,
        ),
    )

    assert unfiltered_rows
    assert filtered_rows
    assert len(filtered_rows) < len(unfiltered_rows)
    assert all(float(row["confidence_pct"]) >= 65.0 for row in filtered_rows)


def test_feature_matrix_includes_benchmark_and_earnings_context() -> None:
    bars = _make_bars(count=80, daily_delta=0.35, volatility_wave=0.02)
    benchmark_bars = _make_bars(count=80, start_close=90.0, daily_delta=0.12, volatility_wave=0.01)
    benchmark_context_by_date = script._build_benchmark_context_by_date(benchmark_bars)
    earnings_date = bars[30].trade_date
    front_iv_series = [None] * len(bars)
    back_iv_series = [None] * len(bars)
    for index in range(len(bars)):
        front_iv_series[index] = 0.18 + (index * 0.001)
        back_iv_series[index] = 0.23 + (index * 0.0012)
    iv_context_by_date = script._build_iv_context_by_date(
        bars,
        front_iv_series=front_iv_series,
        back_iv_series=back_iv_series,
    )
    option_context_by_date = script._build_option_context_by_date(
        bars,
        _make_option_rows_for_trade_date(trade_date=bars[24].trade_date, spot_price=bars[24].close_price),
        front_iv_series=front_iv_series,
    )

    features = script._build_feature_matrix(
        bars,
        benchmark_context_by_date=benchmark_context_by_date,
        earnings_dates={earnings_date},
        option_context_by_date=option_context_by_date,
        iv_context_by_date=iv_context_by_date,
    )

    assert features[24] is not None
    assert len(features[24]) == 38
    assert features[24][14] == 1.0
    assert features[24][15] == 0.0
    assert features[24][23] == 1.0
    assert features[24][24] > 0.0
    assert features[24][25] > 0.0
    assert features[24][26] > 0.0
    assert features[24][37] == 1.0
    assert features[34] is not None
    assert features[34][14] == 0.0
    assert features[34][15] == 1.0
    assert features[34][23] == 0.0
    assert features[34][37] == 1.0


def test_option_context_builds_simple_weekly_option_features() -> None:
    bars = _make_bars(count=80, daily_delta=0.35, volatility_wave=0.02)
    trade_date = bars[24].trade_date
    spot_price = bars[24].close_price
    front_iv_series = [None] * len(bars)
    front_iv_series[24] = 0.24
    option_context_by_date = script._build_option_context_by_date(
        bars,
        _make_option_rows_for_trade_date(trade_date=trade_date, spot_price=spot_price),
        front_iv_series=front_iv_series,
    )

    assert trade_date in option_context_by_date
    (
        front_dte_days,
        front_atm_straddle_pct,
        front_atm_skew_pct,
        put_call_volume_log_ratio,
        put_call_premium_balance,
        option_activity_log_ratio,
        straddle_term_structure_pct,
        has_option_context,
        front_otm_put_iv_pct,
        front_otm_call_iv_pct,
        front_iv_risk_reversal_pct,
        front_iv_butterfly_pct,
        front_otm_put_call_volume_log_ratio,
        front_otm_put_call_premium_balance,
    ) = option_context_by_date[trade_date]
    expected_front_straddle_pct = ((2.2 + 3.1) / spot_price) * 100.0
    expected_front_skew_pct = ((3.1 - 2.2) / spot_price) * 100.0
    expected_term_structure_pct = (((3.8 + 4.6) / spot_price) * 100.0) - expected_front_straddle_pct

    assert front_dte_days == 7.0
    assert front_atm_straddle_pct == pytest.approx(expected_front_straddle_pct)
    assert front_atm_skew_pct == pytest.approx(expected_front_skew_pct)
    assert put_call_volume_log_ratio > 0.0
    assert put_call_premium_balance > 0.0
    assert option_activity_log_ratio > 0.0
    assert straddle_term_structure_pct == pytest.approx(expected_term_structure_pct)
    assert has_option_context == 1.0
    assert front_otm_put_iv_pct > 0.0
    assert front_otm_call_iv_pct > 0.0
    assert front_iv_risk_reversal_pct > 0.0
    assert front_iv_butterfly_pct == pytest.approx(((front_otm_put_iv_pct + front_otm_call_iv_pct) / 2.0) - 24.0)
    assert front_otm_put_call_volume_log_ratio > 0.0
    assert front_otm_put_call_premium_balance > 0.0


def test_iv_context_builds_rank_term_structure_and_change_features() -> None:
    bars = _make_bars(count=80, daily_delta=0.35, volatility_wave=0.02)
    front_iv_series = [None] * len(bars)
    back_iv_series = [None] * len(bars)
    for index in range(len(bars)):
        front_iv_series[index] = 0.18 + (index * 0.001)
        back_iv_series[index] = 0.22 + (index * 0.0015)

    iv_context_by_date = script._build_iv_context_by_date(
        bars,
        front_iv_series=front_iv_series,
        back_iv_series=back_iv_series,
    )

    context = iv_context_by_date[bars[24].trade_date]
    assert context[0] == pytest.approx((0.18 + (24 * 0.001)) * 100.0)
    assert context[1] == pytest.approx((0.22 + (24 * 0.0015)) * 100.0)
    assert context[2] == pytest.approx(context[1] - context[0])
    assert context[3] == pytest.approx(100.0)
    assert context[4] == pytest.approx(0.5)
    assert context[5] == pytest.approx((context[0] + context[1]) / 2.0)
    assert context[6] == 1.0


def test_evaluation_summary_includes_balanced_accuracy_and_confusion_matrix() -> None:
    rows = [
        {"predicted_sign": 1, "actual_sign": 1},
        {"predicted_sign": -1, "actual_sign": -1},
        {"predicted_sign": 1, "actual_sign": -1},
        {"predicted_sign": 0, "actual_sign": 0},
    ]

    summary = script._build_evaluation_summary(rows, total_scorable_dates=8)

    assert summary["observation_count"] == 4
    assert summary["total_scorable_dates"] == 8
    assert summary["abstained_count"] == 4
    assert summary["coverage_pct"] == 50.0
    assert summary["accuracy_pct"] == 75.0
    assert "confusion_matrix" in summary
    assert summary["confusion_matrix"]["1"]["1"] == 1
    assert summary["confusion_matrix"]["-1"]["1"] == 1


def test_select_best_method_name_prefers_accuracy_then_balanced_accuracy() -> None:
    selected = script._select_best_method_name(
        {
            "median25": [
                {"predicted_sign": 1, "actual_sign": 1},
                {"predicted_sign": 1, "actual_sign": -1},
            ],
            "vote25trend": [
                {"predicted_sign": 1, "actual_sign": 1},
                {"predicted_sign": -1, "actual_sign": -1},
            ],
        }
    )

    assert selected == "vote25trend"


def test_count_total_scorable_dates_uses_pre_filter_candidate_pool() -> None:
    bars = _make_bars(count=260, daily_delta=0.25, volatility_wave=0.03)
    features = script._build_feature_matrix(bars)
    candidates = script._build_analog_candidates(bars=bars, features=features, horizon_bars=5)

    total = script._count_total_scorable_dates(
        bars=bars,
        features=features,
        candidates=candidates,
        start_date=bars[40].trade_date,
        horizon_bars=5,
        min_candidate_count=20,
    )

    assert total == 211


def test_method_catalog_keeps_variable_analog_defaults() -> None:
    parser = script.build_parser()
    args = parser.parse_args(["--symbol", "SPY"])

    assert args.max_analogs is None
    assert script._METHOD_NAME_TO_CONFIG["median12trend"].max_analogs == 12
    assert script._METHOD_NAME_TO_CONFIG["median12trend"].same_trend_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["median12rsi"].max_analogs == 12
    assert script._METHOD_NAME_TO_CONFIG["median12rsi"].same_rsi_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["vote12rsi"].max_analogs == 12
    assert script._METHOD_NAME_TO_CONFIG["vote12rsi"].same_rsi_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["vote12trendrsi"].same_trend_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["vote12trendrsi"].same_rsi_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["median15trend"].max_analogs == 15
    assert script._METHOD_NAME_TO_CONFIG["median15trend"].same_trend_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["median15trendrsi"].same_trend_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["median15trendrsi"].same_rsi_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["vote15rsi"].same_rsi_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["vote15trendrsi"].same_trend_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["vote15trendrsi"].same_rsi_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["median20trendrsi"].same_trend_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["median20trendrsi"].same_rsi_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["vote20trendrsi"].same_trend_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["vote20trendrsi"].same_rsi_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["median25trend"].same_trend_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["median25rsi"].same_rsi_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["median25trendrsi"].same_trend_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["median25trendrsi"].same_rsi_bucket is True
    assert script._METHOD_NAME_TO_CONFIG["median40rsi"].max_analogs == 40
    assert script._METHOD_NAME_TO_CONFIG["vote30trend"].max_analogs == 30
    if script._SKLEARN_AVAILABLE:
        assert script._METHOD_NAME_TO_CONFIG["mlgb72"].engine == "ml"
        assert script._METHOD_NAME_TO_CONFIG["mlgbp64"].calibration_method == "platt"


def test_count_total_ml_scorable_dates_respects_minimum_train_rows() -> None:
    if not script._SKLEARN_AVAILABLE:
        pytest.skip("scikit-learn not installed")

    bars = _make_regime_bars()
    features = script._build_feature_matrix(bars)

    total_60 = script._count_total_ml_scorable_dates(
        bars=bars,
        features=features,
        start_date=bars[70].trade_date,
        horizon_bars=5,
        min_train_size=60,
    )
    total_120 = script._count_total_ml_scorable_dates(
        bars=bars,
        features=features,
        start_date=bars[70].trade_date,
        horizon_bars=5,
        min_train_size=120,
    )

    assert total_60 > total_120 > 0


def test_walk_forward_predictions_with_ml_method_include_probability_fields() -> None:
    if not script._SKLEARN_AVAILABLE:
        pytest.skip("scikit-learn not installed")

    bars = _make_regime_bars()
    features = script._build_feature_matrix(bars)
    candidates = script._build_analog_candidates(bars=bars, features=features, horizon_bars=5)

    rows = script._walk_forward_predictions(
        bars=bars,
        features=features,
        candidates=candidates,
        start_date=bars[80].trade_date,
        horizon_bars=5,
        min_spacing_bars=5,
        min_candidate_count=20,
        method=script.PredictionMethodConfig(
            name="mltest",
            vote_mode="ml_classifier",
            engine="ml",
            ml_model_name="logistic_regression",
            confidence_threshold=0.5,
            min_train_size=60,
            retrain_every_bars=10,
        ),
    )

    assert rows
    first_row = rows[0]
    assert first_row["prediction_engine"] == "ml"
    assert first_row["model_name"] == "logistic_regression"
    assert first_row["confidence_pct"] is not None
    assert first_row["train_sample_count"] is not None


def test_walk_forward_predictions_with_calibrated_ml_method_use_calibrated_probabilities() -> None:
    if not script._SKLEARN_AVAILABLE:
        pytest.skip("scikit-learn not installed")

    bars = _make_regime_bars()
    features = script._build_feature_matrix(bars)
    candidates = script._build_analog_candidates(bars=bars, features=features, horizon_bars=5)

    rows = script._walk_forward_predictions(
        bars=bars,
        features=features,
        candidates=candidates,
        start_date=bars[140].trade_date,
        horizon_bars=5,
        min_spacing_bars=5,
        min_candidate_count=20,
        method=script.PredictionMethodConfig(
            name="mlgbp64test",
            vote_mode="ml_classifier",
            engine="ml",
            ml_model_name="gradient_boosting",
            confidence_threshold=0.0,
            min_train_size=120,
            retrain_every_bars=10,
            calibration_method="platt",
            min_calibration_size=30,
        ),
    )

    assert rows
    first_row = rows[0]
    assert first_row["prediction_engine"] == "ml"
    assert first_row["model_name"] == "gradient_boosting_platt"
    assert first_row["confidence_pct"] is not None
    assert first_row["probability_up_pct"] is not None
    assert first_row["probability_down_pct"] is not None
    assert float(first_row["probability_up_pct"]) + float(first_row["probability_down_pct"]) == pytest.approx(100.0)
