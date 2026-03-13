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
        expected_cagr = (((12000.0 / 10000.0) ** (252.0 / 253.0)) - 1.0) * 100.0
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
