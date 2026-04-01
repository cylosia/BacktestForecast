from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError as PydanticValidationError

from backtestforecast.backtests.rules import EntryRuleEvaluator
from backtestforecast.backtests.types import BacktestConfig
from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord
from backtestforecast.schemas.backtests import (
    AdxSeries,
    CloseSeries,
    ComparisonOperator,
    CreateBacktestRunRequest,
    EmaSeries,
    IndicatorLevelCrossRule,
    IndicatorPersistenceRule,
    IndicatorSeriesCrossRule,
    IndicatorThresholdRule,
    IndicatorTrendRule,
    MfiSeries,
    RocSeries,
    RsiSeriesSpec,
    StochasticDSeries,
    StochasticKSeries,
    WilliamsRSeries,
    CciSeries,
)


def _make_bars(
    closes: list[float],
    *,
    volumes: list[float] | None = None,
    start: date = date(2025, 1, 1),
) -> list[DailyBar]:
    vols = volumes or [1_000_000.0 + (i * 10_000.0) for i in range(len(closes))]
    bars: list[DailyBar] = []
    for index, (close, volume) in enumerate(zip(closes, vols, strict=False)):
        trade_date = start + timedelta(days=index)
        bars.append(
            DailyBar(
                trade_date=trade_date,
                open_price=close,
                high_price=close + 1.0,
                low_price=close - 1.0,
                close_price=close,
                volume=volume,
            )
        )
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
    *,
    volumes: list[float] | None = None,
    target_dte: int = 30,
) -> EntryRuleEvaluator:
    bars = _make_bars(closes, volumes=volumes)
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
        earnings_dates=set(),
        option_gateway=StubGateway(),
    )


COMMON_PAYLOAD = {
    "symbol": "AAPL",
    "strategy_type": "long_call",
    "start_date": date(2025, 1, 1),
    "end_date": date(2025, 2, 1),
    "target_dte": 30,
    "dte_tolerance_days": 5,
    "max_holding_days": 20,
    "account_size": Decimal("10000"),
    "risk_per_trade_pct": Decimal("10"),
    "commission_per_contract": Decimal("1"),
}


def test_request_accepts_generic_indicator_rule_payloads() -> None:
    request = CreateBacktestRunRequest(
        **COMMON_PAYLOAD,
        entry_rules=[
            {
                "type": "indicator_trend",
                "series": {"indicator": "rsi", "period": 14},
                "direction": "rising",
                "bars": 4,
            },
            {
                "type": "indicator_level_cross",
                "series": {"indicator": "roc", "period": 5},
                "direction": "crosses_above",
                "level": Decimal("0"),
            },
            {
                "type": "indicator_series_cross",
                "left_series": {"indicator": "close"},
                "right_series": {"indicator": "ema", "period": 10},
                "direction": "crosses_above",
            },
            {
                "type": "indicator_persistence",
                "series": {"indicator": "mfi", "period": 14},
                "operator": "gte",
                "level": Decimal("60"),
                "bars": 3,
            },
            {
                "type": "indicator_threshold",
                "series": {"indicator": "adx", "period": 14},
                "operator": "gte",
                "level": Decimal("20"),
            },
        ],
    )

    assert len(request.entry_rules) == 5
    assert request.entry_rules[0].type == "indicator_trend"
    assert request.entry_rules[2].type == "indicator_series_cross"


def test_build_entry_allowed_mask_matches_generic_rule_evaluation() -> None:
    closes = [100.0] * 8 + [101.0, 102.0, 104.0, 107.0, 111.0, 116.0, 122.0, 129.0]
    volumes = [1_000_000.0 + (index * 50_000.0) for index in range(len(closes))]
    rules = [
        IndicatorTrendRule(
            type="indicator_trend",
            series=RsiSeriesSpec(indicator="rsi", period=5),
            direction="rising",
            bars=3,
        ),
        IndicatorLevelCrossRule(
            type="indicator_level_cross",
            series=RocSeries(indicator="roc", period=3),
            direction="crosses_above",
            level=Decimal("0"),
        ),
        IndicatorSeriesCrossRule(
            type="indicator_series_cross",
            left_series=CloseSeries(indicator="close"),
            right_series=EmaSeries(indicator="ema", period=4),
            direction="crosses_above",
        ),
        IndicatorPersistenceRule(
            type="indicator_persistence",
            series=MfiSeries(indicator="mfi", period=5),
            operator=ComparisonOperator.GTE,
            level=Decimal("50"),
            bars=2,
        ),
    ]

    expected_evaluator = _build_evaluator(closes, rules, volumes=volumes)
    expected = [expected_evaluator.is_entry_allowed(index) for index in range(len(expected_evaluator.bars))]

    mask_evaluator = _build_evaluator(closes, rules, volumes=volumes)
    mask = mask_evaluator.build_entry_allowed_mask()

    assert mask == expected
    assert [mask_evaluator.is_entry_allowed(index) for index in range(len(mask_evaluator.bars))] == expected


def test_indicator_series_cross_rejects_identical_series() -> None:
    with pytest.raises(PydanticValidationError, match="left_series and right_series must be different"):
        CreateBacktestRunRequest(
            **COMMON_PAYLOAD,
            entry_rules=[
                {
                    "type": "indicator_series_cross",
                    "left_series": {"indicator": "ema", "period": 10},
                    "right_series": {"indicator": "ema", "period": 10},
                    "direction": "crosses_above",
                }
            ],
        )


def test_indicator_trend_rsi_rising_triggers() -> None:
    closes = [100.0 - i for i in range(18)] + [82.0 + (i * 1.5) for i in range(15)]
    rule = IndicatorTrendRule(
        type="indicator_trend",
        series=RsiSeriesSpec(indicator="rsi", period=14),
        direction="rising",
        bars=4,
    )
    evaluator = _build_evaluator(closes, [rule])
    triggered = any(evaluator.is_entry_allowed(index) for index in range(18, len(closes)))
    assert triggered is True


def test_indicator_level_cross_roc_crosses_above_zero() -> None:
    closes = [110.0, 108.0, 106.0, 104.0, 102.0, 101.0, 102.0, 104.0, 107.0, 111.0, 116.0]
    rule = IndicatorLevelCrossRule(
        type="indicator_level_cross",
        series=RocSeries(indicator="roc", period=3),
        direction="crosses_above",
        level=Decimal("0"),
    )
    evaluator = _build_evaluator(closes, [rule])
    triggered = any(evaluator.is_entry_allowed(index) for index in range(4, len(closes)))
    assert triggered is True


def test_indicator_series_cross_close_crosses_above_ema() -> None:
    closes = [100.0 - (i * 0.6) for i in range(18)] + [90.0, 92.0, 95.0, 99.0, 104.0, 109.0]
    rule = IndicatorSeriesCrossRule(
        type="indicator_series_cross",
        left_series=CloseSeries(indicator="close"),
        right_series=EmaSeries(indicator="ema", period=8),
        direction="crosses_above",
    )
    evaluator = _build_evaluator(closes, [rule])
    triggered = any(evaluator.is_entry_allowed(index) for index in range(10, len(closes)))
    assert triggered is True


def test_indicator_persistence_mfi_stays_above_threshold() -> None:
    closes = [100.0 + (i * 0.75) for i in range(40)]
    volumes = [1_000_000.0 + (i * 25_000.0) for i in range(40)]
    rule = IndicatorPersistenceRule(
        type="indicator_persistence",
        series=MfiSeries(indicator="mfi", period=14),
        operator=ComparisonOperator.GTE,
        level=Decimal("60"),
        bars=3,
    )
    evaluator = _build_evaluator(closes, [rule], volumes=volumes)
    assert evaluator.is_entry_allowed(len(closes) - 1) is True


def test_new_indicator_series_compute_finite_terminal_values() -> None:
    closes = [100.0 + (i * 0.8) for i in range(80)]
    volumes = [1_000_000.0 + (i * 20_000.0) for i in range(80)]
    evaluator = _build_evaluator(closes, [], volumes=volumes)

    series_expectations = [
        (CciSeries(indicator="cci", period=20), lambda value: value > 0),
        (RocSeries(indicator="roc", period=10), lambda value: value > 0),
        (MfiSeries(indicator="mfi", period=14), lambda value: value >= 50),
        (StochasticKSeries(indicator="stochastic_k", k_period=14, d_period=3, smooth_k=3), lambda value: value >= 50),
        (StochasticDSeries(indicator="stochastic_d", k_period=14, d_period=3, smooth_k=3), lambda value: value >= 50),
        (AdxSeries(indicator="adx", period=14), lambda value: value >= 20),
        (WilliamsRSeries(indicator="williams_r", period=14), lambda value: value > -50),
    ]

    for spec, predicate in series_expectations:
        series = evaluator._get_indicator_series(spec)
        terminal = series[-1]
        assert terminal is not None, f"{spec.indicator} should produce a terminal value"
        assert math.isfinite(terminal), f"{spec.indicator} terminal value should be finite"
        assert predicate(terminal), f"{spec.indicator} terminal value {terminal} failed expectation"


def test_indicator_threshold_rule_with_new_family_series() -> None:
    closes = [100.0 + (i * 0.8) for i in range(80)]
    volumes = [1_000_000.0 + (i * 20_000.0) for i in range(80)]
    rules = [
        IndicatorThresholdRule(
            type="indicator_threshold",
            series=AdxSeries(indicator="adx", period=14),
            operator=ComparisonOperator.GTE,
            level=Decimal("20"),
        ),
        IndicatorThresholdRule(
            type="indicator_threshold",
            series=WilliamsRSeries(indicator="williams_r", period=14),
            operator=ComparisonOperator.GT,
            level=Decimal("-50"),
        ),
    ]
    evaluator = _build_evaluator(closes, rules, volumes=volumes)
    assert evaluator.is_entry_allowed(len(closes) - 1) is True
