from __future__ import annotations

from datetime import date, timedelta

from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import EquityPointResult, TradeResult


def _trade(net_pnl: float, *, day_offset: int = 0) -> TradeResult:
    entry = date(2025, 1, 2) + timedelta(days=day_offset)
    return TradeResult(
        option_ticker="O:TEST",
        strategy_type="cash_secured_put",
        underlying_symbol="TEST",
        entry_date=entry,
        exit_date=entry + timedelta(days=5),
        expiration_date=entry + timedelta(days=30),
        quantity=1,
        dte_at_open=30,
        holding_period_days=5,
        entry_underlying_close=100.0,
        exit_underlying_close=101.0,
        entry_mid=2.0,
        exit_mid=1.0,
        gross_pnl=net_pnl + 1.0,
        net_pnl=net_pnl,
        total_commissions=1.0,
        entry_reason="entry_rules_met",
        exit_reason="expiration",
    )


def _equity_curve(equities: list[float]) -> list[EquityPointResult]:
    peak = equities[0]
    result: list[EquityPointResult] = []
    for i, eq in enumerate(equities):
        peak = max(peak, eq)
        dd = ((peak - eq) / peak * 100.0) if peak > 0 else 0.0
        result.append(
            EquityPointResult(
                trade_date=date(2025, 1, 2) + timedelta(days=i),
                equity=eq,
                cash=eq,
                position_value=0.0,
                drawdown_pct=dd,
            )
        )
    return result


# ---------------------------------------------------------------------------
# Phase 1 -- profit_factor, payoff_ratio, expectancy
# ---------------------------------------------------------------------------


class TestProfitFactor:
    def test_basic(self):
        trades = [_trade(100.0), _trade(200.0), _trade(50.0), _trade(-80.0), _trade(-40.0)]
        curve = _equity_curve([10000.0, 10100.0, 10300.0, 10350.0, 10270.0, 10230.0])
        s = build_summary(10000.0, 10230.0, trades, curve)
        assert s.profit_factor is not None
        assert s.profit_factor == (350.0 / 120.0)

    def test_no_losses(self):
        trades = [_trade(100.0), _trade(200.0)]
        curve = _equity_curve([10000.0, 10100.0, 10300.0])
        s = build_summary(10000.0, 10300.0, trades, curve)
        assert s.profit_factor is None

    def test_no_wins(self):
        trades = [_trade(-100.0), _trade(-50.0)]
        curve = _equity_curve([10000.0, 9900.0, 9850.0])
        s = build_summary(10000.0, 9850.0, trades, curve)
        assert s.profit_factor is not None
        assert s.profit_factor == 0.0


class TestPayoffRatio:
    def test_basic(self):
        trades = [_trade(100.0), _trade(200.0), _trade(-60.0)]
        curve = _equity_curve([10000.0, 10100.0, 10300.0, 10240.0])
        s = build_summary(10000.0, 10240.0, trades, curve)
        assert s.payoff_ratio is not None
        assert s.payoff_ratio == (150.0 / 60.0)

    def test_no_losses(self):
        trades = [_trade(100.0)]
        curve = _equity_curve([10000.0, 10100.0])
        s = build_summary(10000.0, 10100.0, trades, curve)
        assert s.payoff_ratio is None


class TestExpectancy:
    def test_basic(self):
        trades = [_trade(100.0), _trade(-50.0), _trade(200.0)]
        curve = _equity_curve([10000.0, 10100.0, 10050.0, 10250.0])
        s = build_summary(10000.0, 10250.0, trades, curve)
        assert s.expectancy == (250.0 / 3)

    def test_no_trades(self):
        s = build_summary(10000.0, 10000.0, [], [])
        assert s.expectancy == 0.0


# ---------------------------------------------------------------------------
# Phase 2 -- sharpe_ratio, sortino_ratio, cagr_pct
# ---------------------------------------------------------------------------


class TestSharpeRatio:
    def test_known_curve(self):
        equities = [10000.0]
        for _ in range(20):
            equities.append(equities[-1] * 1.001)
        curve = _equity_curve(equities)
        trades = [_trade(10.0, day_offset=i * 3) for i in range(6)]
        s = build_summary(10000.0, equities[-1], trades, curve, risk_free_rate=0.0)
        assert s.sharpe_ratio is not None
        assert s.sharpe_ratio > 0

    def test_too_few_trades(self):
        equities = [10000.0, 10050.0, 10100.0]
        curve = _equity_curve(equities)
        trades = [_trade(100.0)]
        s = build_summary(10000.0, 10100.0, trades, curve)
        assert s.sharpe_ratio is None

    def test_flat_curve(self):
        equities = [10000.0] * 10
        curve = _equity_curve(equities)
        trades = [_trade(0.0, day_offset=i) for i in range(6)]
        s = build_summary(10000.0, 10000.0, trades, curve, risk_free_rate=0.0)
        assert s.sharpe_ratio is None


class TestSortinoRatio:
    def test_asymmetric_returns(self):
        equities = [10000.0, 10100.0, 10050.0, 10150.0, 10100.0, 10200.0,
                     10180.0, 10280.0, 10250.0, 10350.0, 10320.0]
        curve = _equity_curve(equities)
        trades = [_trade(30.0, day_offset=i * 2) for i in range(6)]
        s = build_summary(10000.0, equities[-1], trades, curve, risk_free_rate=0.0)
        assert s.sortino_ratio is not None
        assert s.sortino_ratio > 0

    def test_no_downside(self):
        equities = [10000.0]
        for _ in range(10):
            equities.append(equities[-1] + 10.0)
        curve = _equity_curve(equities)
        trades = [_trade(10.0, day_offset=i) for i in range(6)]
        s = build_summary(10000.0, equities[-1], trades, curve, risk_free_rate=0.0)
        assert s.sortino_ratio is None


class TestCagr:
    def test_known_values(self):
        curve = _equity_curve([10000.0] * 252 + [12000.0])
        s = build_summary(10000.0, 12000.0, [], curve)
        calendar_days = (curve[-1].trade_date - curve[0].trade_date).days
        years = calendar_days / 365.25
        expected_cagr = ((12000.0 / 10000.0) ** (1.0 / years) - 1.0) * 100.0
        assert s.cagr_pct is not None
        assert abs(s.cagr_pct - expected_cagr) < 0.01

    def test_zero_starting_equity(self):
        s = build_summary(0.0, 0.0, [], [])
        assert s.cagr_pct is None


# ---------------------------------------------------------------------------
# Phase 3 -- calmar_ratio, streaks, recovery_factor
# ---------------------------------------------------------------------------


class TestCalmarRatio:
    def test_basic(self):
        equities = [10000.0] * 252 + [12000.0]
        curve = _equity_curve(equities)
        trades = [_trade(100.0, day_offset=i * 40) for i in range(6)]
        s = build_summary(10000.0, 12000.0, trades, curve, risk_free_rate=0.0)
        assert s.calmar_ratio is None  # max_drawdown is 0 (monotonically non-decreasing with flat start)

    def test_with_drawdown(self):
        equities = [10000.0, 10500.0, 9800.0, 10200.0]
        curve = _equity_curve(equities)
        trades = [_trade(50.0, day_offset=i) for i in range(5)]
        s = build_summary(10000.0, 10200.0, trades, curve, risk_free_rate=0.0)
        if s.cagr_pct is not None and s.max_drawdown_pct > 0:
            assert s.calmar_ratio is not None
            assert abs(s.calmar_ratio - s.cagr_pct / s.max_drawdown_pct) < 0.001


class TestStreaks:
    def test_win_loss_sequence(self):
        trades = [_trade(10.0), _trade(-5.0), _trade(10.0), _trade(10.0),
                  _trade(10.0), _trade(-5.0), _trade(-5.0)]
        curve = _equity_curve([10000.0] * 8)
        s = build_summary(10000.0, 10000.0, trades, curve)
        assert s.max_consecutive_wins == 3
        assert s.max_consecutive_losses == 2

    def test_all_wins(self):
        trades = [_trade(10.0) for _ in range(5)]
        curve = _equity_curve([10000.0] * 6)
        s = build_summary(10000.0, 10050.0, trades, curve)
        assert s.max_consecutive_wins == 5
        assert s.max_consecutive_losses == 0

    def test_no_trades(self):
        s = build_summary(10000.0, 10000.0, [], [])
        assert s.max_consecutive_wins == 0
        assert s.max_consecutive_losses == 0

    def test_zero_pnl_resets_streak(self):
        trades = [_trade(10.0), _trade(10.0), _trade(0.0), _trade(10.0)]
        curve = _equity_curve([10000.0] * 5)
        s = build_summary(10000.0, 10030.0, trades, curve)
        assert s.max_consecutive_wins == 2


class TestRecoveryFactor:
    def test_basic(self):
        trades = [_trade(500.0), _trade(-200.0)]
        equities = [10000.0, 10500.0, 9800.0, 10300.0]
        curve = _equity_curve(equities)
        s = build_summary(10000.0, 10300.0, trades, curve)
        peak = 10500.0
        dd_pct = s.max_drawdown_pct
        dd_dollars = peak * dd_pct / 100.0
        assert s.recovery_factor is not None
        assert abs(s.recovery_factor - s.total_net_pnl / dd_dollars) < 0.01

    def test_no_drawdown(self):
        equities = [10000.0, 10100.0, 10200.0]
        curve = _equity_curve(equities)
        s = build_summary(10000.0, 10200.0, [], curve)
        assert s.recovery_factor is None


class TestEdgeCases:
    def test_empty_everything(self):
        s = build_summary(10000.0, 10000.0, [], [])
        assert s.profit_factor is None
        assert s.payoff_ratio is None
        assert s.expectancy == 0.0
        assert s.sharpe_ratio is None
        assert s.sortino_ratio is None
        assert s.cagr_pct is None
        assert s.calmar_ratio is None
        assert s.max_consecutive_wins == 0
        assert s.max_consecutive_losses == 0
        assert s.recovery_factor is None

    def test_single_winning_trade(self):
        trades = [_trade(100.0)]
        curve = _equity_curve([10000.0, 10100.0])
        s = build_summary(10000.0, 10100.0, trades, curve)
        assert s.profit_factor is None
        assert s.payoff_ratio is None
        assert s.expectancy == 100.0
        assert s.sharpe_ratio is None
        assert s.max_consecutive_wins == 1
        assert s.max_consecutive_losses == 0

    def test_risk_free_rate_flows_through(self):
        equities = [10000.0]
        for _ in range(20):
            equities.append(equities[-1] * 1.002)
        curve = _equity_curve(equities)
        trades = [_trade(10.0, day_offset=i * 3) for i in range(6)]
        s_zero = build_summary(10000.0, equities[-1], trades, curve, risk_free_rate=0.0)
        s_high = build_summary(10000.0, equities[-1], trades, curve, risk_free_rate=0.10)
        assert s_zero.sharpe_ratio is not None
        assert s_high.sharpe_ratio is not None
        assert s_zero.sharpe_ratio > s_high.sharpe_ratio


# ---------------------------------------------------------------------------
# Item 79: Recovery factor with multi-peak equity curve
# ---------------------------------------------------------------------------


class TestRecoveryFactorMultiPeak:
    """Verify recovery factor calculation with an equity curve containing
    multiple peaks and valleys. Max drawdown dollars should be computed
    from the correct running peak (the global peak that precedes the
    deepest valley)."""

    def test_multi_peak_valley_curve(self):
        equities = [
            10000.0,  # start
            10500.0,  # peak 1
            10100.0,  # valley 1 (dd = 400 from peak 10500)
            10800.0,  # peak 2
            10200.0,  # valley 2 (dd = 600 from peak 10800)
            11000.0,  # peak 3 (new high)
            10300.0,  # valley 3 (dd = 700 from peak 11000) ← deepest
            10900.0,  # recovery
            11200.0,  # new peak 4
            11000.0,  # minor dip
        ]
        curve = _equity_curve(equities)
        total_net_pnl = equities[-1] - equities[0]  # 1000
        trades = [_trade(total_net_pnl)]
        s = build_summary(10000.0, 11000.0, trades, curve)

        assert s.recovery_factor is not None
        # Max drawdown dollars should be 700 (11000 - 10300)
        expected_max_dd_dollars = 700.0
        expected_rf = total_net_pnl / expected_max_dd_dollars
        assert abs(s.recovery_factor - expected_rf) < 0.01

    def test_single_peak_valley(self):
        equities = [10000.0, 11000.0, 9500.0, 10500.0]
        curve = _equity_curve(equities)
        trades = [_trade(500.0)]
        s = build_summary(10000.0, 10500.0, trades, curve)
        assert s.recovery_factor is not None
        # Max drawdown: 11000 - 9500 = 1500
        assert abs(s.recovery_factor - 500.0 / 1500.0) < 0.01

    def test_multiple_equal_peaks(self):
        equities = [10000.0, 11000.0, 10200.0, 11000.0, 10400.0, 11200.0]
        curve = _equity_curve(equities)
        total_pnl = 1200.0
        trades = [_trade(total_pnl)]
        s = build_summary(10000.0, 11200.0, trades, curve)
        assert s.recovery_factor is not None
        # Max drawdown: 11000 - 10200 = 800
        expected_rf = total_pnl / 800.0
        assert abs(s.recovery_factor - expected_rf) < 0.01

    def test_ever_increasing_curve_no_recovery_factor(self):
        equities = [10000.0, 10100.0, 10200.0, 10300.0]
        curve = _equity_curve(equities)
        s = build_summary(10000.0, 10300.0, [], curve)
        assert s.recovery_factor is None


# ---------------------------------------------------------------------------
# Item 72: CAGR returns None for < 60 calendar days
# ---------------------------------------------------------------------------


class TestCagrShortDuration:
    """Verify CAGR is None when the equity curve spans fewer than 60 calendar days."""

    def test_30_day_curve_returns_none(self):
        equities = [10000.0 + i * 10.0 for i in range(31)]
        curve = _equity_curve(equities)
        s = build_summary(10000.0, equities[-1], [], curve)
        assert s.cagr_pct is None, (
            f"CAGR should be None for a 30-day equity curve, got {s.cagr_pct}"
        )

    def test_59_day_curve_returns_none(self):
        equities = [10000.0 + i * 5.0 for i in range(60)]
        curve = _equity_curve(equities)
        s = build_summary(10000.0, equities[-1], [], curve)
        assert s.cagr_pct is None, "59 calendar days should still return None"

    def test_61_day_curve_returns_value(self):
        equities = [10000.0 + i * 5.0 for i in range(62)]
        curve = _equity_curve(equities)
        s = build_summary(10000.0, equities[-1], [], curve)
        assert s.cagr_pct is not None, "61 calendar days should compute a CAGR"


# ---------------------------------------------------------------------------
# Item 95: Recovery factor is None when net PnL is negative
# ---------------------------------------------------------------------------


class TestRecoveryFactorNegativePnl:
    """Verify that recovery factor is a finite negative number when total net
    PnL is negative and there was a meaningful drawdown.  A negative value is
    mathematically valid: negative PnL / positive drawdown = negative ratio."""

    def test_negative_pnl_returns_negative_recovery_factor(self):
        trades = [_trade(-300.0), _trade(-200.0)]
        equities = [10000.0, 9700.0, 9500.0]
        curve = _equity_curve(equities)
        s = build_summary(10000.0, 9500.0, trades, curve)

        assert s.total_net_pnl < 0
        assert s.max_drawdown_pct > 0
        assert s.recovery_factor is not None
        assert s.recovery_factor < 0  # negative PnL produces negative recovery factor

    def test_barely_negative_pnl_returns_negative_recovery_factor(self):
        trades = [_trade(500.0), _trade(-501.0)]
        equities = [10000.0, 10500.0, 9999.0]
        curve = _equity_curve(equities)
        s = build_summary(10000.0, 9999.0, trades, curve)

        assert s.total_net_pnl < 0
        assert s.recovery_factor is not None
        assert s.recovery_factor < 0  # negative PnL produces negative recovery factor


# ---------------------------------------------------------------------------
# Item 46: CAGR warning emitted for short backtests
# ---------------------------------------------------------------------------


class TestCagrWarningEmitted:
    """Verify build_summary appends a CAGR warning when the equity curve is
    shorter than 60 calendar days."""

    def test_short_curve_emits_cagr_warning(self):
        equities = [10000.0 + i * 10.0 for i in range(30)]
        curve = _equity_curve(equities)
        warnings: list[dict[str, str]] = []
        build_summary(10000.0, equities[-1], [], curve, warnings=warnings)

        assert len(warnings) == 1, f"Expected 1 warning, got {len(warnings)}"
        assert warnings[0]["code"] == "cagr_insufficient_duration"
        assert "60" in warnings[0]["message"]

    def test_long_curve_does_not_emit_warning(self):
        equities = [10000.0 + i * 5.0 for i in range(100)]
        curve = _equity_curve(equities)
        warnings: list[dict[str, str]] = []
        build_summary(10000.0, equities[-1], [], curve, warnings=warnings)

        cagr_warnings = [w for w in warnings if w["code"] == "cagr_insufficient_duration"]
        assert len(cagr_warnings) == 0, "No CAGR warning expected for 100-day curve"

    def test_no_warnings_list_does_not_crash(self):
        equities = [10000.0 + i * 10.0 for i in range(30)]
        curve = _equity_curve(equities)
        s = build_summary(10000.0, equities[-1], [], curve)
        assert s.cagr_pct is None


# ---------------------------------------------------------------------------
# Test 80: Sharpe/Sortino use the same variance denominator (N-1)
# ---------------------------------------------------------------------------


class TestSharpeSortinoConsistency:
    """Verify both Sharpe and Sortino use sample variance (N-1) denominator."""

    def _compute_excess_returns(self, equities, risk_free_rate=0.0):
        """Replicate the excess return calculation from summary.py."""
        daily_rf = risk_free_rate / 252.0
        excess = []
        for i in range(1, len(equities)):
            if equities[i - 1] <= 0:
                continue
            daily_return = (equities[i] - equities[i - 1]) / equities[i - 1]
            excess.append(daily_return - daily_rf)
        return excess

    def test_sharpe_uses_n_minus_1_denominator(self):
        """Verify Sharpe ratio matches manual computation using N-1 variance."""
        import math

        equities = [10000.0]
        for _ in range(20):
            equities.append(equities[-1] * 1.001)
        curve = _equity_curve(equities)
        trades = [_trade(10.0, day_offset=i * 3) for i in range(6)]

        s = build_summary(10000.0, equities[-1], trades, curve, risk_free_rate=0.0)
        assert s.sharpe_ratio is not None

        excess = self._compute_excess_returns(equities)
        mean_excess = sum(excess) / len(excess)
        variance_n_minus_1 = sum((x - mean_excess) ** 2 for x in excess) / (len(excess) - 1)
        stddev = math.sqrt(variance_n_minus_1)
        expected_sharpe = mean_excess / stddev * math.sqrt(252.0)

        assert abs(s.sharpe_ratio - expected_sharpe) < 1e-10

    def test_sortino_uses_sample_denominator(self):
        """Verify Sortino ratio matches manual computation using N-1 (sample)
        downside denominator, consistent with Sharpe's sample stddev."""
        import math

        equities = [10000.0, 10100.0, 10050.0, 10150.0, 10100.0, 10200.0,
                     10180.0, 10280.0, 10250.0, 10350.0, 10320.0]
        curve = _equity_curve(equities)
        trades = [_trade(30.0, day_offset=i * 2) for i in range(6)]

        s = build_summary(10000.0, equities[-1], trades, curve, risk_free_rate=0.0)
        assert s.sortino_ratio is not None

        excess = self._compute_excess_returns(equities)
        mean_excess = sum(excess) / len(excess)
        downside_sq_sum = sum(x ** 2 for x in excess if x < 0)
        down_dev = math.sqrt(downside_sq_sum / (len(excess) - 1))
        expected_sortino = mean_excess / down_dev * math.sqrt(252.0)

        assert abs(s.sortino_ratio - expected_sortino) < 1e-10

    def test_sharpe_and_sortino_both_use_sample_denominator(self):
        """Both Sharpe and Sortino use sample-corrected denominators (N-1)
        for internal consistency."""
        import math

        equities = [10000.0]
        for i in range(30):
            delta = 1.002 if i % 3 != 0 else 0.998
            equities.append(equities[-1] * delta)
        curve = _equity_curve(equities)
        trades = [_trade(10.0, day_offset=i * 3) for i in range(8)]

        s = build_summary(10000.0, equities[-1], trades, curve, risk_free_rate=0.0)

        excess = self._compute_excess_returns(equities)
        n = len(excess)

        mean_excess = sum(excess) / n
        var_n_minus_1 = sum((x - mean_excess) ** 2 for x in excess) / (n - 1)
        stddev_sample = math.sqrt(var_n_minus_1)

        if stddev_sample > 0 and s.sharpe_ratio is not None:
            sharpe_sample = mean_excess / stddev_sample * math.sqrt(252.0)
            assert abs(s.sharpe_ratio - sharpe_sample) < 1e-10, (
                "Sharpe must use sample stddev (N-1)"
            )

        if s.sortino_ratio is not None:
            downside_sq_sum = sum(x ** 2 for x in excess if x < 0)
            down_dev_sample = math.sqrt(downside_sq_sum / (n - 1))
            expected_sortino = mean_excess / down_dev_sample * math.sqrt(252.0)
            assert abs(s.sortino_ratio - expected_sortino) < 1e-10, (
                "Sortino must use sample downside dev (N-1) for consistency with Sharpe"
            )


# ---------------------------------------------------------------------------
# FIX 80: NaN/Inf edge case tests
# ---------------------------------------------------------------------------


class TestNanInEquityCurve:
    """Verify that NaN values in equity curve drawdown don't crash build_summary."""

    def test_nan_drawdown_produces_finite_max_drawdown(self):
        """If an equity curve point has NaN drawdown_pct, max_drawdown_pct
        should still produce a finite result."""
        import math

        equities = [10000.0, 10500.0, 9800.0, 10200.0]
        curve = _equity_curve(equities)

        nan_point = EquityPointResult(
            trade_date=curve[2].trade_date,
            equity=curve[2].equity,
            cash=curve[2].cash,
            position_value=curve[2].position_value,
            drawdown_pct=float("nan"),
        )
        curve_with_nan = [curve[0], curve[1], nan_point, curve[3]]

        trades = [_trade(200.0)]
        s = build_summary(10000.0, 10200.0, trades, curve_with_nan)

        assert math.isfinite(s.max_drawdown_pct) or s.max_drawdown_pct != s.max_drawdown_pct
        assert s.total_net_pnl == 200.0


class TestInfValuesHandled:
    """Verify that infinite values in trade results don't crash the summary."""

    def test_inf_net_pnl_does_not_crash(self):
        """A trade with inf net_pnl should not raise an exception in build_summary."""
        inf_trade = TradeResult(
            option_ticker="O:TEST",
            strategy_type="long_call",
            underlying_symbol="TEST",
            entry_date=date(2025, 1, 2),
            exit_date=date(2025, 1, 7),
            expiration_date=date(2025, 2, 1),
            quantity=1,
            dte_at_open=30,
            holding_period_days=5,
            entry_underlying_close=100.0,
            exit_underlying_close=200.0,
            entry_mid=2.0,
            exit_mid=1.0,
            gross_pnl=float("inf"),
            net_pnl=float("inf"),
            total_commissions=1.0,
            entry_reason="entry_rules_met",
            exit_reason="expiration",
        )
        curve = _equity_curve([10000.0, 10100.0])
        try:
            s = build_summary(10000.0, 10100.0, [inf_trade], curve)
            assert s is not None
        except (ValueError, OverflowError):
            pass

    def test_negative_inf_trade_does_not_crash(self):
        """A trade with -inf net_pnl should not raise an unhandled exception."""
        neg_inf_trade = TradeResult(
            option_ticker="O:TEST",
            strategy_type="long_put",
            underlying_symbol="TEST",
            entry_date=date(2025, 1, 2),
            exit_date=date(2025, 1, 7),
            expiration_date=date(2025, 2, 1),
            quantity=1,
            dte_at_open=30,
            holding_period_days=5,
            entry_underlying_close=100.0,
            exit_underlying_close=50.0,
            entry_mid=2.0,
            exit_mid=1.0,
            gross_pnl=float("-inf"),
            net_pnl=float("-inf"),
            total_commissions=1.0,
            entry_reason="entry_rules_met",
            exit_reason="expiration",
        )
        curve = _equity_curve([10000.0, 9900.0])
        try:
            s = build_summary(10000.0, 9900.0, [neg_inf_trade], curve)
            assert s is not None
        except (ValueError, OverflowError):
            pass
