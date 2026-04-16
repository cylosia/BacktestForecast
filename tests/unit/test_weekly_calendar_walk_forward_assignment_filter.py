from __future__ import annotations

import csv
import io
import json
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import run_weekly_calendar_policy_walk_forward as walk_forward
from backtestforecast.backtests.types import BacktestConfig
from backtestforecast.schemas.backtests import AvoidEarningsRule


def _write_candidate_payload(path: Path, *, median_roi: float, total_roi: float, trade_count: int) -> None:
    path.write_text(
        json.dumps(
            {
                "period": {
                    "start": "2024-01-01",
                    "requested_end": "2025-12-31",
                    "latest_available_date": "2025-12-31",
                },
                "combined_best_result": {
                    "roc_period": 63,
                    "adx_period": 14,
                    "rsi_period": 14,
                    "bull_filter": "roc0_adx10_rsinone",
                    "bear_filter": "roc0_adx14_rsinone",
                    "bull_strategy": "aaa_call_d40_pt50",
                    "bear_strategy": "bear_aaa_call_d30_pt50",
                    "neutral_strategy": "neutral_aaa_call_d40_pt50",
                    "trade_count": trade_count,
                    "total_net_pnl": 1000.0,
                    "total_roi_pct": total_roi,
                    "average_roi_on_margin_pct": median_roi - 5.0,
                    "median_roi_on_margin_pct": median_roi,
                    "win_rate_pct": 60.0,
                    "average_win": 100.0,
                    "average_loss": -50.0,
                },
            }
        ),
        encoding="utf-8",
    )


def test_passes_assignment_filters_respects_put_assignment_thresholds() -> None:
    metrics = {
        "training_assignment_count": 3,
        "training_assignment_rate_pct": 5.0,
        "training_put_assignment_count": 2,
        "training_put_assignment_rate_pct": 3.0,
    }
    assert walk_forward._passes_assignment_filters(
        metrics=metrics,
        max_training_assignment_count=3,
        max_training_assignment_rate_pct=5.0,
        max_training_put_assignment_count=2,
        max_training_put_assignment_rate_pct=3.0,
    )
    assert not walk_forward._passes_assignment_filters(
        metrics=metrics,
        max_training_assignment_count=2,
        max_training_assignment_rate_pct=None,
        max_training_put_assignment_count=None,
        max_training_put_assignment_rate_pct=None,
    )
    assert not walk_forward._passes_assignment_filters(
        metrics=metrics,
        max_training_assignment_count=None,
        max_training_assignment_rate_pct=None,
        max_training_put_assignment_count=1,
        max_training_put_assignment_rate_pct=None,
    )


def test_load_candidates_filters_assignment_risk_and_attaches_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    root = Path("C:/assignment-filter-test-root")
    monkeypatch.setattr(walk_forward, "ROOT", root)

    payload_a = root / "aaa.json"
    payload_b = root / "bbb.json"
    payload_text_by_path: dict[Path, str] = {}
    for path, median_roi, total_roi in (
        (payload_a, 90.0, 10.0),
        (payload_b, 95.0, 12.0),
    ):
        payload_text_by_path[path] = json.dumps(
            {
                "period": {
                    "start": "2024-01-01",
                    "requested_end": "2025-12-31",
                    "latest_available_date": "2025-12-31",
                },
                "combined_best_result": {
                    "roc_period": 63,
                    "adx_period": 14,
                    "rsi_period": 14,
                    "bull_filter": "roc0_adx10_rsinone",
                    "bear_filter": "roc0_adx14_rsinone",
                    "bull_strategy": "aaa_call_d40_pt50",
                    "bear_strategy": "bear_aaa_call_d30_pt50",
                    "neutral_strategy": "neutral_aaa_call_d40_pt50",
                    "trade_count": 100,
                    "total_net_pnl": 1000.0,
                    "total_roi_pct": total_roi,
                    "average_roi_on_margin_pct": median_roi - 5.0,
                    "median_roi_on_margin_pct": median_roi,
                    "win_rate_pct": 60.0,
                    "average_win": 100.0,
                    "average_loss": -50.0,
                },
            }
        )

    real_exists = Path.exists
    real_read_text = Path.read_text

    def _fake_exists(self: Path) -> bool:
        return self in payload_text_by_path or real_exists(self)

    def _fake_read_text(self: Path, *args, **kwargs) -> str:
        if self in payload_text_by_path:
            return payload_text_by_path[self]
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "exists", _fake_exists)
    monkeypatch.setattr(Path, "read_text", _fake_read_text)

    summary_buffer = io.StringIO()
    writer = csv.DictWriter(
        summary_buffer,
        fieldnames=[
            "symbol",
            "status",
            "objective",
            "trade_count",
            "output_path",
            "start_date",
            "requested_end_date",
        ],
    )
    writer.writeheader()
    writer.writerow(
        {
            "symbol": "AAA",
            "status": "completed",
            "objective": "median",
            "trade_count": "100",
            "output_path": "aaa.json",
            "start_date": "2024-01-01",
            "requested_end_date": "2025-12-31",
        }
    )
    writer.writerow(
        {
            "symbol": "BBB",
            "status": "completed",
            "objective": "median",
            "trade_count": "100",
            "output_path": "bbb.json",
            "start_date": "2024-01-01",
            "requested_end_date": "2025-12-31",
        }
    )
    summary_text = summary_buffer.getvalue()

    class _SummaryPath:
        def open(self, *args, **kwargs):
            return io.StringIO(summary_text)

    metrics_by_symbol = {
        "AAA": {
            "training_assignment_count": 0,
            "training_assignment_rate_pct": 0.0,
            "training_put_assignment_count": 0,
            "training_put_assignment_rate_pct": 0.0,
        },
        "BBB": {
            "training_assignment_count": 4,
            "training_assignment_rate_pct": 4.0,
            "training_put_assignment_count": 2,
            "training_put_assignment_rate_pct": 2.0,
        },
    }
    monkeypatch.setattr(
        walk_forward,
        "_load_candidate_training_assignment_metrics",
        lambda candidate: metrics_by_symbol[str(candidate["symbol"])],
    )

    candidates, stats = walk_forward._load_candidates(
        summary_csv=_SummaryPath(),
        train_objective="median",
        min_trade_count=70,
        min_median_roi=None,
        max_training_assignment_count=0,
        max_training_assignment_rate_pct=None,
        max_training_put_assignment_count=None,
        max_training_put_assignment_rate_pct=None,
    )

    assert [candidate["symbol"] for candidate in candidates] == ["AAA"]
    assert candidates[0]["training_assignment_metrics"] == metrics_by_symbol["AAA"]
    assert stats == {
        "base_candidate_count": 2,
        "assignment_filtered_out_count": 1,
    }


def test_resolve_candidate_components_supports_refine_only_profit_and_delta_targets() -> None:
    candidate = {
        "symbol": "SIG",
        "payload": {
            "period": {
                "start": "2024-01-01",
                "requested_end": "2025-12-31",
                "latest_available_date": "2025-12-31",
            }
        },
        "best": {
            "roc_period": 63,
            "adx_period": 14,
            "rsi_period": 14,
            "bull_filter": "roc0_adx10_rsinone",
            "bear_filter": "roc0_adx14_rsinone",
            "bull_strategy": "sig_call_d45_pt80",
            "bear_strategy": "bear_sig_put_d35_pt60",
            "neutral_strategy": "neutral_sig_call_d55_pt70",
        },
    }

    components = walk_forward._resolve_candidate_components(candidate)

    assert components["bull_strategy"].label == "sig_call_d45_pt80"
    assert components["bull_strategy"].delta_target == 45
    assert components["bull_strategy"].profit_target_pct == 80
    assert components["bear_strategy"].label == "bear_sig_put_d35_pt60"
    assert components["bear_strategy"].delta_target == 35
    assert components["bear_strategy"].profit_target_pct == 60
    assert components["neutral_strategy"].label == "neutral_sig_call_d55_pt70"
    assert components["neutral_strategy"].delta_target == 55
    assert components["neutral_strategy"].profit_target_pct == 70


def test_resolve_candidate_components_supports_active_heavy_regime_labels() -> None:
    candidate = {
        "symbol": "AGQ",
        "payload": {
            "period": {
                "start": "2024-01-01",
                "requested_end": "2025-12-31",
                "latest_available_date": "2025-12-31",
            }
        },
        "best": {
            "regime_mode": "best_regime_only",
            "active_regime": "heavy_bearish",
            "roc_period": 63,
            "adx_period": 14,
            "rsi_period": 14,
            "bull_filter": "roc0_adx10_rsinone",
            "bear_filter": "roc0_adx14_rsinone",
            "heavy_bull_strategy": "agq_call_d35_pt80",
            "bull_strategy": "agq_call_d45_pt80",
            "bear_strategy": "bear_agq_put_d35_pt60",
            "heavy_bear_strategy": "bear_agq_put_d25_pt70",
            "neutral_strategy": "neutral_agq_call_d55_pt70",
        },
    }

    components = walk_forward._resolve_candidate_components(candidate)

    assert components["regime_mode"] == "best_regime_only"
    assert components["active_regime"] == "heavy_bearish"
    assert components["heavy_bull_strategy"].label == "agq_call_d35_pt80"
    assert components["heavy_bear_strategy"].label == "bear_agq_put_d25_pt70"


def test_select_regime_strategy_for_candidate_supports_heavy_and_regular_branches() -> None:
    bull_filter = walk_forward._build_default_bull_filters()[0]
    bear_filter = walk_forward._build_default_bear_filters()[0]
    heavy_bull_strategy = SimpleNamespace(label="agq_call_d30_pt75")
    bull_strategy = SimpleNamespace(label="agq_call_d40_pt50")
    bear_strategy = SimpleNamespace(label="bear_agq_put_d40_pt50")
    heavy_bear_strategy = SimpleNamespace(label="bear_agq_put_d30_pt75")
    neutral_strategy = SimpleNamespace(label="neutral_agq_call_d40_pt50")

    regime, strategy = walk_forward._select_regime_strategy_for_candidate(
        indicator_row={"roc63": 8.0, "adx14": 16.0, "rsi14": 70.0},
        bull_filter=bull_filter,
        bear_filter=bear_filter,
        heavy_bull_strategy=heavy_bull_strategy,
        bull_strategy=bull_strategy,
        bear_strategy=bear_strategy,
        heavy_bear_strategy=heavy_bear_strategy,
        neutral_strategy=neutral_strategy,
    )
    assert (regime, strategy.label) == ("heavy_bullish", "agq_call_d30_pt75")

    regime, strategy = walk_forward._select_regime_strategy_for_candidate(
        indicator_row={"roc63": -8.0, "adx14": 25.0, "rsi14": 30.0},
        bull_filter=bull_filter,
        bear_filter=bear_filter,
        heavy_bull_strategy=heavy_bull_strategy,
        bull_strategy=bull_strategy,
        bear_strategy=bear_strategy,
        heavy_bear_strategy=heavy_bear_strategy,
        neutral_strategy=neutral_strategy,
    )
    assert (regime, strategy.label) == ("heavy_bearish", "bear_agq_put_d30_pt75")

    regime, strategy = walk_forward._select_regime_strategy_for_candidate(
        indicator_row={"roc63": 2.0, "adx14": 12.0, "rsi14": 55.0},
        bull_filter=bull_filter,
        bear_filter=bear_filter,
        heavy_bull_strategy=heavy_bull_strategy,
        bull_strategy=bull_strategy,
        bear_strategy=bear_strategy,
        heavy_bear_strategy=heavy_bear_strategy,
        neutral_strategy=neutral_strategy,
    )
    assert (regime, strategy.label) == ("bullish", "agq_call_d40_pt50")


def test_load_candidate_training_stability_metrics_aggregates_monthly_medians(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate: dict[str, object] = {}
    monkeypatch.setattr(
        walk_forward,
        "_load_candidate_training_trade_rows",
        lambda _candidate: [
            {"_entry_date": date(2024, 1, 5), "roi_on_margin_pct": 10.0},
            {"_entry_date": date(2024, 1, 12), "roi_on_margin_pct": 30.0},
            {"_entry_date": date(2024, 2, 2), "roi_on_margin_pct": -20.0},
            {"_entry_date": date(2024, 2, 9), "roi_on_margin_pct": 40.0},
            {"_entry_date": date(2024, 3, 1), "roi_on_margin_pct": 50.0},
        ],
    )

    metrics = walk_forward._load_candidate_training_stability_metrics(candidate)

    assert metrics == {
        "training_months_with_trades": 3,
        "training_positive_month_count": 3,
        "training_negative_month_count": 0,
        "training_worst_month_median_roi_pct": 10.0,
        "training_p25_monthly_median_roi_pct": 15.0,
        "training_median_monthly_median_roi_pct": 20.0,
        "training_best_month_median_roi_pct": 50.0,
    }
    assert candidate["training_stability_metrics"] == metrics


def test_apply_stability_filter_reranks_survivors_by_monthly_consistency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidates = [
        {
            "symbol": "AAA",
            "best": {
                "median_roi_on_margin_pct": 95.0,
                "average_roi_on_margin_pct": 90.0,
                "total_roi_pct": 3.0,
                "win_rate_pct": 70.0,
                "trade_count": 100,
            },
        },
        {
            "symbol": "BBB",
            "best": {
                "median_roi_on_margin_pct": 90.0,
                "average_roi_on_margin_pct": 85.0,
                "total_roi_pct": 2.0,
                "win_rate_pct": 68.0,
                "trade_count": 95,
            },
        },
        {
            "symbol": "CCC",
            "best": {
                "median_roi_on_margin_pct": 85.0,
                "average_roi_on_margin_pct": 80.0,
                "total_roi_pct": 1.0,
                "win_rate_pct": 66.0,
                "trade_count": 90,
            },
        },
    ]
    metrics_by_symbol = {
        "AAA": {
            "training_positive_month_count": 21,
            "training_p25_monthly_median_roi_pct": 30.0,
            "training_median_monthly_median_roi_pct": 70.0,
        },
        "BBB": {
            "training_positive_month_count": 22,
            "training_p25_monthly_median_roi_pct": 40.0,
            "training_median_monthly_median_roi_pct": 60.0,
        },
        "CCC": {
            "training_positive_month_count": 20,
            "training_p25_monthly_median_roi_pct": 50.0,
            "training_median_monthly_median_roi_pct": 80.0,
        },
    }
    monkeypatch.setattr(
        walk_forward,
        "_load_candidate_training_stability_metrics",
        lambda candidate: metrics_by_symbol[str(candidate["symbol"])],
    )

    survivors, stats = walk_forward._apply_stability_filter(
        candidates=candidates,
        stability_top_pool=3,
        stability_min_positive_months=21,
        stability_min_p25_monthly_median_roi_pct=25.0,
        train_objective="median",
    )

    assert [candidate["symbol"] for candidate in survivors] == ["BBB", "AAA"]
    assert stats == {
        "stability_filter_enabled": True,
        "stability_top_pool_count": 3,
        "stability_filtered_out_count": 1,
        "stability_survivor_count": 2,
    }


def test_apply_stability_filter_returns_original_candidates_when_disabled() -> None:
    candidates = [{"symbol": "AAA"}, {"symbol": "BBB"}]

    survivors, stats = walk_forward._apply_stability_filter(
        candidates=candidates,
        stability_top_pool=0,
        stability_min_positive_months=21,
        stability_min_p25_monthly_median_roi_pct=25.0,
        train_objective="median",
    )

    assert survivors == candidates
    assert stats == {
        "stability_filter_enabled": False,
        "stability_top_pool_count": 0,
        "stability_filtered_out_count": 0,
        "stability_survivor_count": 2,
    }


def test_build_replay_calendar_config_applies_stop_loss_override(monkeypatch: pytest.MonkeyPatch) -> None:
    base_config = BacktestConfig(
        symbol="AAA",
        strategy_type="calendar_spread",
        start_date=date(2026, 1, 2),
        end_date=date(2026, 2, 6),
        target_dte=7,
        dte_tolerance_days=3,
        max_holding_days=10,
        account_size=Decimal("100000"),
        risk_per_trade_pct=Decimal("100"),
        commission_per_contract=Decimal("0"),
        entry_rules=[],
        profit_target_pct=50.0,
        stop_loss_pct=None,
    )
    monkeypatch.setattr(walk_forward.two_stage, "_build_calendar_config", lambda **_: base_config)

    config = walk_forward._build_replay_calendar_config(
        strategy=SimpleNamespace(label="aaa_call_d40_pt50"),
        entry_date=date(2026, 1, 2),
        latest_available_date=date(2026, 4, 13),
        risk_free_curve=SimpleNamespace(default_rate=0.04),
        stop_loss_pct=15.0,
        avoid_earnings_days_before=0,
        avoid_earnings_days_after=0,
    )

    assert config.stop_loss_pct == 15.0
    assert config.profit_target_pct == 50.0
    assert base_config.stop_loss_pct is None


def test_build_replay_calendar_config_appends_avoid_earnings_rule(monkeypatch: pytest.MonkeyPatch) -> None:
    base_config = BacktestConfig(
        symbol="AAA",
        strategy_type="calendar_spread",
        start_date=date(2026, 1, 2),
        end_date=date(2026, 2, 6),
        target_dte=7,
        dte_tolerance_days=3,
        max_holding_days=10,
        account_size=Decimal("100000"),
        risk_per_trade_pct=Decimal("100"),
        commission_per_contract=Decimal("0"),
        entry_rules=[],
        profit_target_pct=50.0,
        stop_loss_pct=None,
    )
    monkeypatch.setattr(walk_forward.two_stage, "_build_calendar_config", lambda **_: base_config)

    config = walk_forward._build_replay_calendar_config(
        strategy=SimpleNamespace(label="aaa_call_d40_pt50"),
        entry_date=date(2026, 1, 2),
        latest_available_date=date(2026, 4, 13),
        risk_free_curve=SimpleNamespace(default_rate=0.04),
        stop_loss_pct=None,
        avoid_earnings_days_before=7,
        avoid_earnings_days_after=2,
    )

    assert len(config.entry_rules) == 1
    assert isinstance(config.entry_rules[0], AvoidEarningsRule)
    assert config.entry_rules[0].days_before == 7
    assert config.entry_rules[0].days_after == 2
    assert base_config.entry_rules == []


def test_calculate_trade_roi_medians_reports_weighted_and_unweighted_values() -> None:
    weighted, unweighted = walk_forward._calculate_trade_roi_medians(
        [10.0, 20.0, 30.0, 40.0],
        [0.6, 0.2, 0.1, 0.1],
    )

    assert weighted == 10.0
    assert unweighted == 25.0


def test_parse_args_defaults_to_earnings_blackout_10_before_and_after(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["run_weekly_calendar_policy_walk_forward.py"])

    args = walk_forward._parse_args()

    assert args.avoid_earnings_days_before == 10
    assert args.avoid_earnings_days_after == 10


def test_install_assignment_exit_ignore_filter_suppresses_selected_reasons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_method = walk_forward.OptionsBacktestEngine._check_early_assignment
    monkeypatch.setattr(
        walk_forward.OptionsBacktestEngine,
        "_check_early_assignment",
        original_method,
    )

    def _fake_original(cls, *, position, bar, ex_dividend_dates):
        return getattr(bar, "reason", None), {"reason": getattr(bar, "reason", None)}

    monkeypatch.setattr(walk_forward, "_ORIGINAL_CHECK_EARLY_ASSIGNMENT", _fake_original)

    ignored = walk_forward._install_assignment_exit_ignore_filter(
        ignored_assignment_exit_reasons=[
            "early_assignment_call_ex_div,early_assignment_put_deep_itm",
            "early_assignment_call_ex_div",
        ]
    )

    assert ignored == (
        "early_assignment_call_ex_div",
        "early_assignment_put_deep_itm",
    )
    assert walk_forward.OptionsBacktestEngine._check_early_assignment(
        position=object(),
        bar=SimpleNamespace(reason="early_assignment_call_ex_div"),
        ex_dividend_dates=set(),
    ) == (None, None)
    assert walk_forward.OptionsBacktestEngine._check_early_assignment(
        position=object(),
        bar=SimpleNamespace(reason="expiration"),
        ex_dividend_dates=set(),
    ) == ("expiration", {"reason": "expiration"})


def test_apply_spot_price_filter_excludes_low_spot_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    candidates = [
        {"symbol": "AAA"},
        {"symbol": "BBB"},
        {"symbol": "CCC"},
    ]
    monkeypatch.setattr(
        walk_forward,
        "_load_latest_spot_price_metrics",
        lambda **_: {
            "AAA": {
                "selection_spot_trade_date": "2026-04-13",
                "selection_spot_price": 10.0,
            },
            "BBB": {
                "selection_spot_trade_date": "2026-04-13",
                "selection_spot_price": 4.99,
            },
        },
    )

    survivors, stats = walk_forward._apply_spot_price_filter(
        candidates=candidates,
        min_spot_price=5.0,
        as_of_date=date(2026, 4, 13),
    )

    assert [candidate["symbol"] for candidate in survivors] == ["AAA"]
    assert survivors[0]["selection_spot_metrics"] == {
        "selection_spot_trade_date": "2026-04-13",
        "selection_spot_price": 10.0,
    }
    assert stats == {
        "spot_filter_enabled": True,
        "spot_filter_as_of_date": "2026-04-13",
        "spot_filtered_out_count": 2,
        "spot_missing_count": 1,
    }


def test_default_output_prefix_includes_runtime_override_suffixes(monkeypatch: pytest.MonkeyPatch) -> None:
    root = Path("C:/walk-forward-test-root")
    monkeypatch.setattr(walk_forward, "ROOT", root)

    assert walk_forward._default_output_prefix(
        top_k=22,
        stop_loss_pct=None,
        min_spot_price=None,
        avoid_earnings_days_before=0,
        avoid_earnings_days_after=0,
        ignored_assignment_exit_reasons=(),
    ) == (
        root / "logs" / "weekly_calendar_policy_walk_forward_top22_train20251231_q1_2026"
    )
    assert walk_forward._default_output_prefix(
        top_k=22,
        stop_loss_pct=15.0,
        min_spot_price=None,
        avoid_earnings_days_before=0,
        avoid_earnings_days_after=0,
        ignored_assignment_exit_reasons=(),
    ) == (
        root / "logs" / "weekly_calendar_policy_walk_forward_top22_train20251231_q1_2026_sl15"
    )
    assert walk_forward._default_output_prefix(
        top_k=22,
        stop_loss_pct=None,
        min_spot_price=5.0,
        avoid_earnings_days_before=0,
        avoid_earnings_days_after=0,
        ignored_assignment_exit_reasons=(),
    ) == (
        root / "logs" / "weekly_calendar_policy_walk_forward_top22_train20251231_q1_2026_minspot5"
    )
    assert walk_forward._default_output_prefix(
        top_k=22,
        stop_loss_pct=None,
        min_spot_price=None,
        avoid_earnings_days_before=7,
        avoid_earnings_days_after=2,
        ignored_assignment_exit_reasons=(),
    ) == (
        root / "logs" / "weekly_calendar_policy_walk_forward_top22_train20251231_q1_2026_earningsb7a2"
    )
    assert walk_forward._default_output_prefix(
        top_k=22,
        stop_loss_pct=None,
        min_spot_price=None,
        avoid_earnings_days_before=0,
        avoid_earnings_days_after=0,
        ignored_assignment_exit_reasons=(
            "early_assignment_call_ex_div",
            "early_assignment_put_deep_itm",
        ),
    ) == (
        root
        / "logs"
        / "weekly_calendar_policy_walk_forward_top22_train20251231_q1_2026_ignoreassign_call_ex_div__put_deep_itm"
    )
