"""Verify serialize_summary handles Infinity and NaN without crashing."""
from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

from backtestforecast.schemas.analysis import DailyPickSummary
from backtestforecast.schemas.backtests import BacktestSummaryResponse
from backtestforecast.services.exports import ExportService
from backtestforecast.services.serialization import _opt_decimal, _safe_decimal, serialize_summary
from backtestforecast.utils import to_decimal


def _make_summary(**overrides):
    defaults = dict(
        trade_count=10, decided_trades=10, win_rate=60.0, total_roi_pct=15.0,
        average_win_amount=200.0, average_loss_amount=-100.0,
        average_holding_period_days=5.0, average_dte_at_open=30.0,
        max_drawdown_pct=8.0, total_commissions=50.0,
        total_net_pnl=1500.0, starting_equity=10000.0,
        ending_equity=11500.0, profit_factor=2.5,
        payoff_ratio=2.0, expectancy=150.0,
        sharpe_ratio=1.2, sortino_ratio=1.8,
        cagr_pct=25.0, calmar_ratio=3.1,
        max_consecutive_wins=5, max_consecutive_losses=2,
        recovery_factor=1.5,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestInfinityHandling:
    def test_profit_factor_infinity_serializes_as_explicit_string(self):
        summary = _make_summary(profit_factor=float("inf"))
        result = serialize_summary(summary)
        assert result["profit_factor"] == "Infinity"
        json.dumps(result)  # must not raise

    def test_sharpe_ratio_infinity_serializes_as_explicit_string(self):
        summary = _make_summary(sharpe_ratio=float("inf"))
        result = serialize_summary(summary)
        assert result["sharpe_ratio"] == "Infinity"
        json.dumps(result)

    def test_negative_infinity_serializes_as_explicit_string(self):
        summary = _make_summary(sortino_ratio=float("-inf"))
        result = serialize_summary(summary)
        assert result["sortino_ratio"] == "-Infinity"
        json.dumps(result)

    def test_nan_win_rate_serializes_as_zero(self):
        summary = _make_summary(win_rate=float("nan"))
        result = serialize_summary(summary)
        assert result["win_rate"] == 0.0
        json.dumps(result)

    def test_nan_profit_factor_serializes_as_none(self):
        summary = _make_summary(profit_factor=float("nan"))
        result = serialize_summary(summary)
        assert result["profit_factor"] is None
        json.dumps(result)

    def test_all_infinity_fields_produce_valid_json(self):
        summary = _make_summary(
            profit_factor=float("inf"),
            payoff_ratio=float("inf"),
            sharpe_ratio=float("-inf"),
            sortino_ratio=float("nan"),
            cagr_pct=float("inf"),
            calmar_ratio=float("-inf"),
            recovery_factor=float("nan"),
        )
        result = serialize_summary(summary)
        serialized = json.dumps(result)
        parsed = json.loads(serialized)
        assert parsed["profit_factor"] == "Infinity"
        assert parsed["payoff_ratio"] == "Infinity"
        assert parsed["sharpe_ratio"] == "-Infinity"
        assert parsed["sortino_ratio"] is None
        assert parsed["cagr_pct"] == "Infinity"
        assert parsed["calmar_ratio"] == "-Infinity"
        assert parsed["recovery_factor"] is None


class TestSafeDecimalDirect:
    def test_safe_decimal_inf_returns_zero(self):
        assert _safe_decimal(float("inf")) == 0.0

    def test_safe_decimal_nan_returns_zero(self):
        assert _safe_decimal(float("nan")) == 0.0

    def test_safe_decimal_normal_value(self):
        assert abs(_safe_decimal(42.5) - 42.5) < 0.001

    def test_opt_decimal_inf_returns_explicit_string(self):
        assert _opt_decimal(float("inf")) == "Infinity"

    def test_opt_decimal_neg_inf_returns_explicit_string(self):
        assert _opt_decimal(float("-inf")) == "-Infinity"

    def test_opt_decimal_nan_returns_none(self):
        assert _opt_decimal(float("nan")) is None

    def test_opt_decimal_none_returns_none(self):
        assert _opt_decimal(None) is None

    def test_opt_decimal_normal_value(self):
        result = _opt_decimal(42.5)
        assert result is not None
        assert abs(result - 42.5) < 0.001


class TestToDecimalAllowInfinite:
    def test_allow_infinite_preserves_positive_infinity(self):
        assert to_decimal(float("inf"), allow_infinite=True) == Decimal("Infinity")

    def test_allow_infinite_preserves_negative_infinity(self):
        assert to_decimal(float("-inf"), allow_infinite=True) == Decimal("-Infinity")


class TestResponseSchemas:
    def test_backtest_summary_response_accepts_and_json_serializes_infinite_metrics(self):
        summary = BacktestSummaryResponse.model_validate({
            "trade_count": 1,
            "total_commissions": 0,
            "total_net_pnl": 1,
            "starting_equity": 1,
            "ending_equity": 2,
            "win_rate": 1,
            "total_roi_pct": 1,
            "max_drawdown_pct": 1,
            "profit_factor": Decimal("Infinity"),
            "payoff_ratio": "Infinity",
            "recovery_factor": Decimal("-Infinity"),
        })

        dumped = summary.model_dump(mode="json")
        assert dumped["profit_factor"] == "Infinity"
        assert dumped["payoff_ratio"] == "Infinity"
        assert dumped["recovery_factor"] == "-Infinity"

    def test_daily_pick_summary_preserves_json_safe_infinite_metrics(self):
        summary = DailyPickSummary.model_validate({
            "profit_factor": "Infinity",
            "sharpe_ratio": Decimal("-Infinity"),
        })

        dumped = summary.model_dump(mode="json")
        assert dumped["profit_factor"] == "Infinity"
        assert dumped["sharpe_ratio"] == "-Infinity"

    def test_export_metric_formatter_preserves_explicit_infinity_strings(self):
        assert ExportService._format_metric_value("Infinity") == "Infinity"
        assert ExportService._format_metric_value("-Infinity", percent=True) == "-Infinity"
        assert ExportService._format_metric_value(float("inf"), usd=True) == "Infinity"
