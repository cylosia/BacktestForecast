"""Tests for EntryRuleEvaluator covering all 8 rule types."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from backtestforecast.backtests.rules import EntryRuleEvaluator
from backtestforecast.backtests.types import BacktestConfig
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord
from backtestforecast.schemas.backtests import (
    AvoidEarningsRule,
    BollingerBand,
    BollingerBandsRule,
    ComparisonOperator,
    IvPercentileRule,
    IvRankRule,
    MacdRule,
    MovingAverageCrossoverRule,
    RsiRule,
    SupportResistanceMode,
    SupportResistanceRule,
    VolumeSpikeRule,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_bars(
    closes: list[float],
    volumes: list[float] | None = None,
    start: date = date(2025, 1, 1),
) -> list[DailyBar]:
    vols = volumes or [1_000_000.0] * len(closes)
    bars = []
    for i, (c, v) in enumerate(zip(closes, vols)):
        d = start + timedelta(days=i)
        bars.append(DailyBar(trade_date=d, open_price=c, high_price=c, low_price=c, close_price=c, volume=v))
    return bars


@dataclass
class StubGateway:
    iv_values: list[float | None] | None = None

    def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
        if self.iv_values is None:
            return []
        exp = entry_date + timedelta(days=target_dte)
        return [OptionContractRecord(f"OPT_{entry_date}", contract_type, exp, 100.0, 100)]

    def select_contract(self, entry_date, strategy_type, underlying_close, target_dte, dte_tolerance_days):
        return self.list_contracts(entry_date, "call", target_dte, dte_tolerance_days)[0]

    def get_quote(self, option_ticker, trade_date):
        return OptionQuoteRecord(trade_date=trade_date, bid_price=2.0, ask_price=2.0, participant_timestamp=None)


def _build_evaluator(
    closes: list[float],
    rules: list,
    volumes: list[float] | None = None,
    earnings_dates: set[date] | None = None,
    target_dte: int = 30,
    gateway: StubGateway | None = None,
) -> EntryRuleEvaluator:
    bars = _make_bars(closes, volumes)
    config = BacktestConfig(
        symbol="TEST",
        strategy_type="long_call",
        start_date=bars[0].trade_date,
        end_date=bars[-1].trade_date,
        target_dte=target_dte,
        dte_tolerance_days=10,
        max_holding_days=30,
        account_size=10_000,
        risk_per_trade_pct=5,
        commission_per_contract=1,
        entry_rules=rules,
    )
    return EntryRuleEvaluator(
        config=config,
        bars=bars,
        earnings_dates=earnings_dates or set(),
        option_gateway=gateway or StubGateway(),
    )


# ---------------------------------------------------------------------------
# 1. RSI
# ---------------------------------------------------------------------------


def test_rsi_lte_triggers():
    closes = [100.0] * 5 + [100 - i * 2 for i in range(25)]
    rule = RsiRule(type="rsi", operator=ComparisonOperator.LTE, threshold=Decimal("30"), period=14)
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(len(closes) - 1) is True


def test_rsi_gte_blocks():
    closes = [100.0] * 5 + [100 - i * 2 for i in range(25)]
    rule = RsiRule(type="rsi", operator=ComparisonOperator.GTE, threshold=Decimal("70"), period=14)
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(len(closes) - 1) is False


def test_rsi_insufficient_data():
    closes = [100.0, 101.0, 99.0]
    rule = RsiRule(type="rsi", operator=ComparisonOperator.LTE, threshold=Decimal("30"), period=14)
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(2) is False


# ---------------------------------------------------------------------------
# 2. SMA Crossover
# ---------------------------------------------------------------------------


def test_sma_crossover_golden_cross():
    down = [100 - i * 0.5 for i in range(20)]
    sharp_up = [90 + i * 3 for i in range(15)]
    closes = down + sharp_up
    rule = MovingAverageCrossoverRule(
        type="sma_crossover", fast_period=3, slow_period=10, direction="bullish"
    )
    ev = _build_evaluator(closes, [rule])
    triggered = any(ev.is_entry_allowed(i) for i in range(20, len(closes)))
    assert triggered is True


def test_sma_crossover_no_cross():
    closes = [100.0 - i * 0.5 for i in range(30)]
    rule = MovingAverageCrossoverRule(
        type="sma_crossover", fast_period=5, slow_period=15, direction="bullish"
    )
    ev = _build_evaluator(closes, [rule])
    triggered = any(ev.is_entry_allowed(i) for i in range(1, len(closes)))
    assert triggered is False


# ---------------------------------------------------------------------------
# 3. EMA Crossover
# ---------------------------------------------------------------------------


def test_ema_crossover_bullish():
    closes = [50.0] * 30 + [50 + i * 2 for i in range(15)]
    rule = MovingAverageCrossoverRule(
        type="ema_crossover", fast_period=5, slow_period=20, direction="bullish"
    )
    ev = _build_evaluator(closes, [rule])
    triggered = any(ev.is_entry_allowed(i) for i in range(30, len(closes)))
    assert triggered is True


# ---------------------------------------------------------------------------
# 4. MACD
# ---------------------------------------------------------------------------


def test_macd_bullish_crossover():
    closes = [100.0] * 30 + [100 + i for i in range(15)]
    rule = MacdRule(type="macd", direction="bullish", fast_period=5, slow_period=12, signal_period=4)
    ev = _build_evaluator(closes, [rule])
    triggered = any(ev.is_entry_allowed(i) for i in range(30, len(closes)))
    assert triggered is True


def test_macd_insufficient_data():
    closes = [100.0, 101.0, 99.0]
    rule = MacdRule(type="macd", direction="bullish", fast_period=5, slow_period=12, signal_period=4)
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(2) is False


# ---------------------------------------------------------------------------
# 5. Bollinger Bands
# ---------------------------------------------------------------------------


def test_bollinger_below_lower_band():
    closes = [100.0] * 25 + [80.0]
    rule = BollingerBandsRule(
        type="bollinger_bands",
        band=BollingerBand.LOWER,
        operator=ComparisonOperator.LTE,
        period=20,
        standard_deviations=Decimal("2"),
    )
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(len(closes) - 1) is True


def test_bollinger_above_upper_band():
    closes = [100.0] * 25 + [120.0]
    rule = BollingerBandsRule(
        type="bollinger_bands",
        band=BollingerBand.UPPER,
        operator=ComparisonOperator.GTE,
        period=20,
        standard_deviations=Decimal("2"),
    )
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(len(closes) - 1) is True


# ---------------------------------------------------------------------------
# 6. IV Rank / IV Percentile
# ---------------------------------------------------------------------------


def test_iv_rank_high(monkeypatch):
    closes = [100.0] * 50
    rule = IvRankRule(
        type="iv_rank", operator=ComparisonOperator.GTE, threshold=Decimal("50"), lookback_days=30
    )

    iv_series = [0.2 + i * 0.005 for i in range(50)]
    import backtestforecast.backtests.rules as rules_mod

    monkeypatch.setattr(rules_mod, "build_estimated_iv_series", lambda **kwargs: iv_series)

    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(49) is True


def test_iv_insufficient_history(monkeypatch):
    closes = [100.0] * 10
    rule = IvRankRule(
        type="iv_rank", operator=ComparisonOperator.GTE, threshold=Decimal("50"), lookback_days=252
    )

    iv_series = [None] * 5 + [0.25] * 5
    import backtestforecast.backtests.rules as rules_mod

    monkeypatch.setattr(rules_mod, "build_estimated_iv_series", lambda **kwargs: iv_series)

    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(9) is False


def test_iv_percentile_high(monkeypatch):
    closes = [100.0] * 50
    rule = IvPercentileRule(
        type="iv_percentile", operator=ComparisonOperator.GTE, threshold=Decimal("50"), lookback_days=30
    )

    iv_series = [0.2 + i * 0.005 for i in range(50)]
    import backtestforecast.backtests.rules as rules_mod

    monkeypatch.setattr(rules_mod, "build_estimated_iv_series", lambda **kwargs: iv_series)

    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(49) is True


def test_iv_percentile_low(monkeypatch):
    closes = [100.0] * 50
    rule = IvPercentileRule(
        type="iv_percentile", operator=ComparisonOperator.LTE, threshold=Decimal("20"), lookback_days=30
    )

    iv_series = [0.2 + i * 0.005 for i in range(50)]
    import backtestforecast.backtests.rules as rules_mod

    monkeypatch.setattr(rules_mod, "build_estimated_iv_series", lambda **kwargs: iv_series)

    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(49) is False


# ---------------------------------------------------------------------------
# 7. Volume Spike
# ---------------------------------------------------------------------------


def test_volume_spike_detected():
    vols = [1_000_000.0] * 25 + [5_000_000.0]
    closes = [100.0] * 26
    rule = VolumeSpikeRule(
        type="volume_spike",
        operator=ComparisonOperator.GTE,
        multiplier=Decimal("2.0"),
        lookback_period=20,
    )
    ev = _build_evaluator(closes, [rule], volumes=vols)
    assert ev.is_entry_allowed(25) is True


def test_volume_spike_no_spike():
    vols = [1_000_000.0] * 26
    closes = [100.0] * 26
    rule = VolumeSpikeRule(
        type="volume_spike",
        operator=ComparisonOperator.GTE,
        multiplier=Decimal("2.0"),
        lookback_period=20,
    )
    ev = _build_evaluator(closes, [rule], volumes=vols)
    assert ev.is_entry_allowed(25) is False


def test_volume_baseline_zero():
    vols = [0.0] * 25 + [1_000_000.0]
    closes = [100.0] * 26
    rule = VolumeSpikeRule(
        type="volume_spike",
        operator=ComparisonOperator.GTE,
        multiplier=Decimal("1.5"),
        lookback_period=20,
    )
    ev = _build_evaluator(closes, [rule], volumes=vols)
    assert ev.is_entry_allowed(25) is False


# ---------------------------------------------------------------------------
# 8. Support / Resistance
# ---------------------------------------------------------------------------


def test_near_support():
    closes = [100.0, 95.0, 98.0, 94.0, 97.0, 93.0, 96.0, 93.2]
    rule = SupportResistanceRule(
        type="support_resistance",
        mode=SupportResistanceMode.NEAR_SUPPORT,
        lookback_period=5,
        tolerance_pct=Decimal("2.0"),
    )
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(len(closes) - 1) is True


def test_breakout():
    closes = [100.0] * 10 + [101.0, 102.0, 103.0, 104.0, 105.0, 110.0]
    rule = SupportResistanceRule(
        type="support_resistance",
        mode=SupportResistanceMode.BREAKOUT_ABOVE_RESISTANCE,
        lookback_period=10,
        tolerance_pct=Decimal("1.0"),
    )
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(len(closes) - 1) is True


def test_zero_support_returns_false():
    closes = [0.0] * 10 + [1.0]
    rule = SupportResistanceRule(
        type="support_resistance",
        mode=SupportResistanceMode.NEAR_SUPPORT,
        lookback_period=5,
        tolerance_pct=Decimal("2.0"),
    )
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(10) is False


def test_rsi_lt_operator():
    closes = [100 - i * 2 for i in range(20)]
    rule = RsiRule(type="rsi", operator=ComparisonOperator.LT, threshold=Decimal("30"), period=14)
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(19) is True


def test_rsi_gt_operator():
    closes = [100 + i * 2 for i in range(20)]
    rule = RsiRule(type="rsi", operator=ComparisonOperator.GT, threshold=Decimal("70"), period=14)
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(19) is True


def test_near_resistance():
    closes = [95.0, 100.0, 97.0, 101.0, 98.0, 102.0, 99.0, 101.5]
    rule = SupportResistanceRule(
        type="support_resistance",
        mode=SupportResistanceMode.NEAR_RESISTANCE,
        lookback_period=5,
        tolerance_pct=Decimal("2.0"),
    )
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(len(closes) - 1) is True


def test_breakdown_below_support():
    closes = [100.0] * 10 + [99.0, 98.0, 97.0, 96.0, 95.0, 85.0]
    rule = SupportResistanceRule(
        type="support_resistance",
        mode=SupportResistanceMode.BREAKDOWN_BELOW_SUPPORT,
        lookback_period=10,
        tolerance_pct=Decimal("1.0"),
    )
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(len(closes) - 1) is True


# ---------------------------------------------------------------------------
# 9. Avoid Earnings
# ---------------------------------------------------------------------------


def test_avoid_earnings_blocks_near_earnings():
    closes = [100.0] * 10
    bars = _make_bars(closes)
    earnings = {bars[5].trade_date}
    rule = AvoidEarningsRule(type="avoid_earnings", days_before=2, days_after=2)
    ev = _build_evaluator(closes, [rule], earnings_dates=earnings)
    assert ev.is_entry_allowed(5) is False
    assert ev.is_entry_allowed(4) is False


def test_avoid_earnings_allows_outside_window():
    closes = [100.0] * 20
    bars = _make_bars(closes)
    earnings = {bars[5].trade_date}
    rule = AvoidEarningsRule(type="avoid_earnings", days_before=2, days_after=2)
    ev = _build_evaluator(closes, [rule], earnings_dates=earnings)
    assert ev.is_entry_allowed(15) is True


# ---------------------------------------------------------------------------
# 10. Integration: is_entry_allowed
# ---------------------------------------------------------------------------


def test_multiple_rules_all_must_pass():
    vols = [1_000_000.0] * 25 + [5_000_000.0]
    closes = [100.0] * 5 + [100 - i * 2 for i in range(21)]
    rsi_rule = RsiRule(type="rsi", operator=ComparisonOperator.LTE, threshold=Decimal("30"), period=14)
    vol_rule = VolumeSpikeRule(
        type="volume_spike",
        operator=ComparisonOperator.GTE,
        multiplier=Decimal("2.0"),
        lookback_period=20,
    )
    ev = _build_evaluator(closes, [rsi_rule, vol_rule], volumes=vols)
    assert ev.is_entry_allowed(25) is True


def test_index_zero_returns_false():
    closes = [100.0] * 5
    rule = RsiRule(type="rsi", operator=ComparisonOperator.LTE, threshold=Decimal("90"), period=2)
    ev = _build_evaluator(closes, [rule])
    assert ev.is_entry_allowed(0) is False
