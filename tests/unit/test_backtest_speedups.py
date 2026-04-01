from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.strategies.common import (
    build_contract_delta_lookup,
    maybe_build_contract_delta_lookup,
    require_contract_for_strike,
)
from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import (
    BacktestConfig,
    BacktestExecutionResult,
    EquityPointResult,
    OpenMultiLegPosition,
    OpenOptionLeg,
)
from backtestforecast.market_data.types import OptionContractRecord, OptionQuoteRecord
from backtestforecast.schemas.backtests import ComparisonOperator, RsiRule
from backtestforecast.domain.execution_parameters import ResolvedExecutionParameters
from backtestforecast.market_data.service import HistoricalDataBundle
from backtestforecast.schemas.backtests import CreateBacktestRunRequest
from backtestforecast.services.backtest_execution import BacktestExecutionService


class _StubMarketDataService:
    def __init__(self, bundle: HistoricalDataBundle) -> None:
        self._bundle = bundle
        self.client = SimpleNamespace(close=lambda: None)
        self._redis_cache = None

    def prepare_backtest(self, request: CreateBacktestRunRequest) -> HistoricalDataBundle:
        return self._bundle


class _CapturingEngine:
    def run(self, *, config, bars, earnings_dates, ex_dividend_dates, option_gateway):
        curve = [
            EquityPointResult(
                trade_date=bars[0].trade_date,
                equity=Decimal("10000"),
                cash=Decimal("10000"),
                position_value=Decimal("0"),
                drawdown_pct=Decimal("0"),
            ),
            EquityPointResult(
                trade_date=bars[-1].trade_date,
                equity=Decimal("10100"),
                cash=Decimal("10100"),
                position_value=Decimal("0"),
                drawdown_pct=Decimal("0"),
            ),
        ]
        summary = build_summary(10000.0, 10100.0, [], curve, risk_free_rate=config.risk_free_rate)
        return BacktestExecutionResult(summary=summary, trades=[], equity_curve=curve, warnings=[])


def test_execution_service_prefetches_contracts_for_single_backtests(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    captured: dict[str, object] = {}

    def _fake_prewarm(
        request,
        *,
        bundle,
        include_quotes,
        max_dates,
        warm_future_quotes,
    ):
        captured["symbol"] = request.symbol
        captured["bar_count"] = len(bundle.bars)
        captured["start_date"] = request.start_date
        captured["end_date"] = request.end_date
        captured["target_dte"] = request.target_dte
        captured["dte_tolerance_days"] = request.dte_tolerance_days
        captured["include_quotes"] = include_quotes
        captured["max_dates"] = max_dates
        captured["warm_future_quotes"] = warm_future_quotes
        return SimpleNamespace(to_dict=lambda: {"dates_processed": len(bundle.bars)})

    settings = SimpleNamespace(
        option_cache_warn_age_seconds=259_200,
        backtest_option_prefetch_enabled=True,
        backtest_prefetch_min_trade_dates=2,
        backtest_prefetch_max_dates=4,
        backtest_prefetch_timeout_seconds=77,
    )
    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, "prewarm_long_option_bundle", _fake_prewarm)
    monkeypatch.setattr(module, "build_backtest_risk_free_rate_curve", lambda *args, **kwargs: None)

    bars = [
        SimpleNamespace(trade_date=date(2025, 4, 1)),
        SimpleNamespace(trade_date=date(2025, 4, 2)),
        SimpleNamespace(trade_date=date(2025, 4, 3)),
    ]
    bundle = HistoricalDataBundle(
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=SimpleNamespace(),
    )
    service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle),
        engine=_CapturingEngine(),
    )
    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date="2025-04-01",
        end_date="2025-04-03",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
    )
    resolved = ResolvedExecutionParameters(
        risk_free_rate=0.01,
        risk_free_rate_source="configured_fallback",
        risk_free_rate_field_name="yield_3_month",
        risk_free_rate_model="curve_default",
        dividend_yield=0.0,
        source_of_truth="test",
    )

    result = service.execute_request(request, resolved_parameters=resolved)

    assert result.summary.ending_equity == 10100.0
    assert captured["symbol"] == "AAPL"
    assert captured["include_quotes"] is True
    assert captured["max_dates"] == 4
    assert captured["warm_future_quotes"] is True


def test_engine_mark_position_uses_batch_quote_fetch_when_available() -> None:
    engine = OptionsBacktestEngine()
    position = OpenMultiLegPosition(
        display_ticker="O:TEST",
        strategy_type="calendar_spread",
        underlying_symbol="AAPL",
        entry_date=date(2025, 4, 1),
        entry_index=0,
        quantity=1,
        dte_at_open=7,
        option_legs=[
            OpenOptionLeg("O:LEG1", "call", 1, 100.0, date(2025, 4, 18), 1, 1.0, 1.0),
            OpenOptionLeg("O:LEG2", "call", -1, 105.0, date(2025, 4, 18), 1, 0.5, 0.5),
        ],
    )
    bar = SimpleNamespace(trade_date=date(2025, 4, 2), close_price=102.0)

    class _Gateway:
        def get_quotes(self, option_tickers, trade_date):
            assert option_tickers == ["O:LEG1", "O:LEG2"]
            return {
                "O:LEG1": OptionQuoteRecord(trade_date, 1.2, 1.4, None),
                "O:LEG2": OptionQuoteRecord(trade_date, 0.6, 0.8, None),
            }

        def get_quote(self, option_ticker, trade_date):
            raise AssertionError("single-ticker quote path should not be used")

    snapshot = engine._mark_position(
        position,
        bar,
        _Gateway(),
        warnings=[],
        warning_codes=set(),
        ex_dividend_dates=set(),
    )

    assert position.option_legs[0].last_mid == pytest.approx(1.3)
    assert position.option_legs[1].last_mid == pytest.approx(0.7)
    assert snapshot.position_missing_quote is False


def test_execution_service_logs_phase_timings(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    logged: dict[str, object] = {}

    settings = SimpleNamespace(
        option_cache_warn_age_seconds=259_200,
        backtest_option_prefetch_enabled=False,
        backtest_prefetch_min_trade_dates=2,
        backtest_prefetch_max_dates=4,
        backtest_prefetch_timeout_seconds=77,
    )
    perf_samples = iter([0.0, 0.001, 0.003, 0.004, 0.006, 0.007, 0.010, 0.011, 0.019, 0.020, 0.021, 0.022])

    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, "_time", SimpleNamespace(perf_counter=lambda: next(perf_samples)))
    monkeypatch.setattr(module, "_logger", SimpleNamespace(info=lambda event, **kwargs: logged.update({"event": event, **kwargs})))
    monkeypatch.setattr(module, "build_backtest_risk_free_rate_curve", lambda *args, **kwargs: None)

    bars = [
        SimpleNamespace(trade_date=date(2025, 4, 1)),
        SimpleNamespace(trade_date=date(2025, 4, 2)),
        SimpleNamespace(trade_date=date(2025, 4, 3)),
    ]
    bundle = HistoricalDataBundle(
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=SimpleNamespace(),
        data_source="historical_flatfile",
    )
    service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle),
        engine=_CapturingEngine(),
    )
    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date="2025-04-01",
        end_date="2025-04-03",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
    )
    resolved = ResolvedExecutionParameters(
        risk_free_rate=0.01,
        risk_free_rate_source="configured_fallback",
        risk_free_rate_field_name="yield_3_month",
        risk_free_rate_model="curve_default",
        dividend_yield=0.0,
        source_of_truth="test",
    )

    service.execute_request(request, bundle=bundle, resolved_parameters=resolved)

    assert logged["event"] == "backtest.execute_timing"
    assert logged["used_prepared_bundle"] is True
    assert logged["data_source"] == "historical_flatfile"
    assert logged["prepare_ms"] == 2.0
    assert logged["prefetch_ms"] == 2.0
    assert logged["parameter_ms"] == 3.0
    assert logged["engine_ms"] == 8.0
    assert logged["staleness_ms"] == 1.0
    assert logged["total_ms"] == 22.0


def test_execution_service_uses_targeted_exact_prefetch_for_supported_non_long_strategies(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    captured: dict[str, object] = {}

    def _fake_targeted_prewarm(
        request,
        *,
        bundle,
        include_quotes,
        max_dates,
        warm_future_quotes,
    ):
        captured["symbol"] = request.symbol
        captured["bar_count"] = len(bundle.bars)
        captured["start_date"] = request.start_date
        captured["end_date"] = request.end_date
        captured["target_dte"] = request.target_dte
        captured["dte_tolerance_days"] = request.dte_tolerance_days
        captured["include_quotes"] = include_quotes
        captured["max_dates"] = max_dates
        captured["warm_future_quotes"] = warm_future_quotes
        return SimpleNamespace(to_dict=lambda: {"dates_processed": len(bundle.bars)})

    settings = SimpleNamespace(
        option_cache_warn_age_seconds=259_200,
        backtest_option_prefetch_enabled=True,
        backtest_prefetch_min_trade_dates=2,
        backtest_prefetch_max_dates=4,
        backtest_prefetch_timeout_seconds=77,
    )
    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, "prewarm_targeted_option_bundle", _fake_targeted_prewarm)
    monkeypatch.setattr(module, "build_backtest_risk_free_rate_curve", lambda *args, **kwargs: None)

    bars = [
        SimpleNamespace(trade_date=date(2025, 4, 1)),
        SimpleNamespace(trade_date=date(2025, 4, 2)),
        SimpleNamespace(trade_date=date(2025, 4, 3)),
    ]
    bundle = HistoricalDataBundle(
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=SimpleNamespace(),
    )
    service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle),
        engine=_CapturingEngine(),
    )
    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="covered_call",
        start_date="2025-04-01",
        end_date="2025-04-03",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
    )
    resolved = ResolvedExecutionParameters(
        risk_free_rate=0.01,
        risk_free_rate_source="configured_fallback",
        risk_free_rate_field_name="yield_3_month",
        risk_free_rate_model="curve_default",
        dividend_yield=0.0,
        source_of_truth="test",
    )

    result = service.execute_request(request, resolved_parameters=resolved)

    assert result.summary.ending_equity == 10100.0
    assert captured["symbol"] == "AAPL"
    assert captured["include_quotes"] is False
    assert captured["max_dates"] == 4
    assert captured["warm_future_quotes"] is False


def test_execution_service_skips_duplicate_prefetch_for_warm_bundle(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    prewarm_calls: list[str] = []
    logged_events: list[str] = []

    def _fake_targeted_prewarm(
        request,
        *,
        bundle,
        include_quotes,
        max_dates,
        warm_future_quotes,
    ):
        prewarm_calls.append(request.strategy_type.value)
        return SimpleNamespace(to_dict=lambda: {"dates_processed": 2, "contracts_fetched": 10, "quotes_fetched": 0, "errors": []})

    settings = SimpleNamespace(
        option_cache_warn_age_seconds=259_200,
        backtest_option_prefetch_enabled=True,
        backtest_prefetch_min_trade_dates=2,
        backtest_prefetch_max_dates=4,
        backtest_prefetch_timeout_seconds=77,
    )
    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, "prewarm_targeted_option_bundle", _fake_targeted_prewarm)
    monkeypatch.setattr(module, "build_backtest_risk_free_rate_curve", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        module,
        "_logger",
        SimpleNamespace(
            info=lambda event, **kwargs: logged_events.append(event),
            warning=lambda *args, **kwargs: None,
        ),
    )

    bars = [
        SimpleNamespace(trade_date=date(2025, 4, 1)),
        SimpleNamespace(trade_date=date(2025, 4, 2)),
        SimpleNamespace(trade_date=date(2025, 4, 3)),
    ]
    bundle = HistoricalDataBundle(
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=SimpleNamespace(),
    )
    service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle),
        engine=_CapturingEngine(),
    )
    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="covered_call",
        start_date="2025-04-01",
        end_date="2025-04-03",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
        profit_target_pct=Decimal("50"),
    )
    resolved = ResolvedExecutionParameters(
        risk_free_rate=0.01,
        risk_free_rate_source="configured_fallback",
        risk_free_rate_field_name="yield_3_month",
        risk_free_rate_model="curve_default",
        dividend_yield=0.0,
        source_of_truth="test",
    )

    service.execute_request(request, bundle=bundle, resolved_parameters=resolved)
    service.execute_request(request, bundle=bundle, resolved_parameters=resolved)

    assert prewarm_calls == ["covered_call"]
    assert "backtest.option_prefetch_skipped" in logged_events


def test_execution_service_prefetch_requests_with_shared_bundle_deduplicates(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    prewarm_calls: list[tuple[str, Decimal | None]] = []

    def _fake_targeted_prewarm(
        request,
        *,
        bundle,
        include_quotes,
        max_dates,
        warm_future_quotes,
    ):
        prewarm_calls.append((request.strategy_type.value, request.profit_target_pct))
        return SimpleNamespace(to_dict=lambda: {"dates_processed": 2, "contracts_fetched": 10, "quotes_fetched": 0, "errors": []})

    settings = SimpleNamespace(
        option_cache_warn_age_seconds=259_200,
        backtest_option_prefetch_enabled=True,
        backtest_prefetch_min_trade_dates=2,
        backtest_prefetch_max_dates=4,
        backtest_prefetch_timeout_seconds=77,
    )
    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, "prewarm_targeted_option_bundle", _fake_targeted_prewarm)
    monkeypatch.setattr(module, "_logger", SimpleNamespace(info=lambda *args, **kwargs: None, warning=lambda *args, **kwargs: None))

    bars = [
        SimpleNamespace(trade_date=date(2025, 4, 1)),
        SimpleNamespace(trade_date=date(2025, 4, 2)),
        SimpleNamespace(trade_date=date(2025, 4, 3)),
    ]
    bundle = HistoricalDataBundle(
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=SimpleNamespace(),
    )
    service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle),
        engine=_CapturingEngine(),
    )
    request_one = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="covered_call",
        start_date="2025-04-01",
        end_date="2025-04-03",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[],
        profit_target_pct=Decimal("50"),
    )
    request_two = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="covered_call",
        start_date="2025-04-01",
        end_date="2025-04-03",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[],
        profit_target_pct=Decimal("75"),
    )

    summary = service.prefetch_requests_with_shared_bundle([request_one, request_two], bundle=bundle)

    assert prewarm_calls == [("covered_call", Decimal("50"))]
    assert summary["prefetch_count"] == 1
    assert summary["skipped_count"] == 1
    assert summary["dates_processed"] == 2
    assert len(summary["requests"]) == 2


def test_execution_service_keeps_broad_prefetch_for_complex_strategies(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    captured: dict[str, object] = {}

    class _FakePrefetcher:
        def __init__(self, timeout_seconds: int) -> None:
            captured["timeout_seconds"] = timeout_seconds

        def prefetch_for_symbol(
            self,
            symbol,
            bars,
            start_date,
            end_date,
            target_dte,
            dte_tolerance_days,
            option_gateway,
            *,
            include_quotes,
            max_dates,
        ):
            captured["symbol"] = symbol
            captured["bar_count"] = len(bars)
            captured["start_date"] = start_date
            captured["end_date"] = end_date
            captured["target_dte"] = target_dte
            captured["dte_tolerance_days"] = dte_tolerance_days
            captured["include_quotes"] = include_quotes
            captured["max_dates"] = max_dates
            return SimpleNamespace(to_dict=lambda: {"dates_processed": len(bars)})

    settings = SimpleNamespace(
        option_cache_warn_age_seconds=259_200,
        backtest_option_prefetch_enabled=True,
        backtest_prefetch_min_trade_dates=2,
        backtest_prefetch_max_dates=4,
        backtest_prefetch_timeout_seconds=77,
    )
    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, "OptionDataPrefetcher", _FakePrefetcher)
    monkeypatch.setattr(module, "build_backtest_risk_free_rate_curve", lambda *args, **kwargs: None)

    bars = [
        SimpleNamespace(trade_date=date(2025, 4, 1)),
        SimpleNamespace(trade_date=date(2025, 4, 2)),
        SimpleNamespace(trade_date=date(2025, 4, 3)),
    ]
    bundle = HistoricalDataBundle(
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=SimpleNamespace(),
    )
    service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle),
        engine=_CapturingEngine(),
    )
    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="diagonal_spread",
        start_date="2025-04-01",
        end_date="2025-04-03",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
    )
    resolved = ResolvedExecutionParameters(
        risk_free_rate=0.01,
        risk_free_rate_source="configured_fallback",
        risk_free_rate_field_name="yield_3_month",
        risk_free_rate_model="curve_default",
        dividend_yield=0.0,
        source_of_truth="test",
    )

    result = service.execute_request(request, resolved_parameters=resolved)

    assert result.summary.ending_equity == 10100.0
    assert captured["symbol"] == "AAPL"
    assert captured["include_quotes"] is False
    assert captured["max_dates"] == 4
    assert captured["timeout_seconds"] == 77


@dataclass
class _ExpensiveStrategy:
    strategy_type: str = "expensive_strategy"
    margin_warning_message: str | None = None
    build_calls: int = 0

    def estimate_minimum_capital_required_per_unit(self, config: BacktestConfig, bar) -> float | None:
        return 50_000.0

    def build_position(self, config, bar, bar_index, option_gateway):
        self.build_calls += 1
        raise AssertionError("build_position should not be reached when capital is obviously insufficient")


def test_engine_skips_chain_lookup_when_strategy_minimum_capital_cannot_be_met(monkeypatch) -> None:
    import backtestforecast.backtests.engine as engine_module

    strategy = _ExpensiveStrategy()
    monkeypatch.setitem(engine_module.STRATEGY_REGISTRY, strategy.strategy_type, strategy)

    config = BacktestConfig(
        symbol="AAPL",
        strategy_type=strategy.strategy_type,
        start_date=date(2025, 4, 1),
        end_date=date(2025, 4, 3),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("1000"),
        risk_per_trade_pct=Decimal("1"),
        commission_per_contract=Decimal("1"),
        entry_rules=[],
        risk_free_rate=0.01,
        dividend_yield=0.0,
        slippage_pct=0.0,
    )
    bars = [
        SimpleNamespace(
            trade_date=date(2025, 4, 1) + timedelta(days=i),
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.0,
            volume=1_000_000.0,
        )
        for i in range(3)
    ]

    result = OptionsBacktestEngine().run(
        config=config,
        bars=bars,
        earnings_dates=set(),
        option_gateway=SimpleNamespace(),
        ex_dividend_dates=set(),
    )

    assert strategy.build_calls == 0
    assert any(w["code"] == "capital_requirement_exceeded" for w in result.warnings)


def test_engine_uses_precomputed_entry_mask(monkeypatch) -> None:
    import backtestforecast.backtests.engine as engine_module

    @dataclass
    class _NoopStrategy:
        strategy_type: str = "noop_strategy"
        margin_warning_message: str | None = None

        def estimate_minimum_capital_required_per_unit(self, config: BacktestConfig, bar) -> float | None:
            return None

        def build_position(self, config, bar, bar_index, option_gateway):
            raise AssertionError("build_position should not be reached when precomputed mask blocks entries")

    strategy = _NoopStrategy()
    monkeypatch.setitem(engine_module.STRATEGY_REGISTRY, strategy.strategy_type, strategy)
    monkeypatch.setattr(
        engine_module.EntryRuleEvaluator,
        "build_entry_allowed_mask",
        lambda self: [False] * len(self.bars),
    )
    monkeypatch.setattr(
        engine_module.EntryRuleEvaluator,
        "is_entry_allowed",
        lambda self, index: (_ for _ in ()).throw(AssertionError("per-bar evaluation should not be used")),
    )

    config = BacktestConfig(
        symbol="AAPL",
        strategy_type=strategy.strategy_type,
        start_date=date(2025, 4, 1),
        end_date=date(2025, 4, 3),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[
            RsiRule(type="rsi", operator=ComparisonOperator.LTE, threshold=Decimal("35"), period=14)
        ],
        risk_free_rate=0.01,
        dividend_yield=0.0,
        slippage_pct=0.0,
    )
    bars = [
        SimpleNamespace(
            trade_date=date(2025, 4, 1) + timedelta(days=i),
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.0,
            volume=1_000_000.0,
        )
        for i in range(3)
    ]

    result = OptionsBacktestEngine().run(
        config=config,
        bars=bars,
        earnings_dates=set(),
        option_gateway=SimpleNamespace(),
        ex_dividend_dates=set(),
    )

    assert not any(w["code"] == "entry_rule_evaluation_error" for w in result.warnings)


def test_require_contract_for_strike_preserves_first_match_when_strikes_repeat_across_expirations():
    contracts = [
        OptionContractRecord("O:F250404P00015000", "put", date(2025, 4, 4), 15.0, 100.0),
        OptionContractRecord("O:F250411P00015000", "put", date(2025, 4, 11), 15.0, 100.0),
    ]

    first = require_contract_for_strike(contracts, 15.0)
    second = require_contract_for_strike(contracts, 15.0)

    assert first.ticker == "O:F250404P00015000"
    assert second.ticker == "O:F250404P00015000"


def test_build_contract_delta_lookup_falls_back_to_one_quote_per_contract_when_gateway_has_no_chain_snapshot():
    class _Gateway:
        def __init__(self) -> None:
            self.quote_calls: list[tuple[str, date]] = []

        def get_chain_delta_lookup(self, contracts):
            return {}

        def get_quote(self, option_ticker: str, trade_date: date):
            self.quote_calls.append((option_ticker, trade_date))
            return OptionQuoteRecord(trade_date=trade_date, bid_price=2.0, ask_price=2.2, participant_timestamp=None)

        def get_iv(self, key):
            return False, None

        def store_iv(self, key, value):
            return None

    gateway = _Gateway()
    contracts = [
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
        OptionContractRecord("O:AAPL250404C00210000", "call", date(2025, 4, 4), 210.0, 100.0),
    ]

    lookup = build_contract_delta_lookup(
        contracts=contracts,
        option_gateway=gateway,
        trade_date=date(2025, 4, 1),
        underlying_close=205.0,
        dte_days=3,
        risk_free_rate=0.01,
    )

    assert set(lookup) == {
        (200.0, date(2025, 4, 4)),
        (210.0, date(2025, 4, 4)),
    }
    assert gateway.quote_calls == [
        ("O:AAPL250404C00200000", date(2025, 4, 1)),
        ("O:AAPL250404C00210000", date(2025, 4, 1)),
    ]


def test_maybe_build_contract_delta_lookup_skips_work_for_non_delta_selection():
    class _Gateway:
        def get_chain_delta_lookup(self, contracts):
            raise AssertionError("delta lookup should not be built for default nearest-OTM selection")

        def get_quote(self, option_ticker: str, trade_date: date):
            raise AssertionError("quote lookup should not be used for default nearest-OTM selection")

    contracts = [
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
    ]

    assert maybe_build_contract_delta_lookup(
        selection=None,
        contracts=contracts,
        option_gateway=_Gateway(),
        trade_date=date(2025, 4, 1),
        underlying_close=205.0,
        dte_days=3,
        risk_free_rate=0.01,
    ) is None


def test_worker_backtest_service_reuses_shared_execution_service(monkeypatch) -> None:
    import apps.worker.app.tasks as tasks_module

    class _FakeExecutionService:
        init_count = 0

        def __init__(self, market_data_service=None) -> None:
            type(self).init_count += 1
            self.market_data_service = market_data_service

    fake_market_data_service = MagicMock()
    fake_market_data_service.client = MagicMock()
    monkeypatch.setattr(
        "backtestforecast.services.backtest_execution.MarketDataService",
        lambda client: fake_market_data_service,
    )
    monkeypatch.setattr("backtestforecast.services.backtest_execution.MassiveClient", lambda: object())
    monkeypatch.setattr("backtestforecast.services.backtest_execution.BacktestExecutionService", _FakeExecutionService)
    tasks_module.close_shared_backtest_execution_service()

    first = tasks_module._get_shared_backtest_execution_service()
    second = tasks_module._get_shared_backtest_execution_service()

    assert first is second
    assert _FakeExecutionService.init_count == 1
    tasks_module.close_shared_backtest_execution_service()


def test_worker_service_builders_reuse_shared_execution_service_without_closing_it(monkeypatch) -> None:
    import apps.worker.app.tasks as tasks_module

    shared_execution_service = MagicMock()
    shared_execution_service.market_data_service = MagicMock()

    monkeypatch.setattr(tasks_module, "_get_shared_backtest_execution_service", lambda: shared_execution_service)
    monkeypatch.delenv("BFF_TEST_FAKE_BACKTEST_EXECUTION", raising=False)

    scan_service = tasks_module._build_scan_service(MagicMock())
    sweep_service = tasks_module._build_sweep_service(MagicMock())
    multi_symbol_service = tasks_module._build_multi_symbol_backtest_service(MagicMock())
    multi_step_service = tasks_module._build_multi_step_backtest_service(MagicMock())

    assert scan_service.execution_service is shared_execution_service
    assert sweep_service.execution_service is shared_execution_service
    assert multi_symbol_service.execution_service is shared_execution_service
    assert multi_step_service.execution_service is shared_execution_service

    scan_service.close()
    sweep_service.close()
    multi_symbol_service.close()
    multi_step_service.close()

    shared_execution_service.close.assert_not_called()


def test_direct_services_reuse_thread_local_execution_service_without_closing_it(monkeypatch) -> None:
    from backtestforecast.services.backtests import BacktestService
    from backtestforecast.services.multi_step_backtests import MultiStepBacktestService
    from backtestforecast.services.multi_symbol_backtests import MultiSymbolBacktestService
    from backtestforecast.services.scans import ScanService
    from backtestforecast.services.sweeps import SweepService

    shared_execution_service = MagicMock()
    shared_execution_service.market_data_service = MagicMock()

    monkeypatch.setattr(
        "backtestforecast.services.backtests.get_thread_local_shared_execution_service",
        lambda: shared_execution_service,
    )
    monkeypatch.setattr(
        "backtestforecast.services.scans.get_thread_local_shared_execution_service",
        lambda: shared_execution_service,
    )
    monkeypatch.setattr(
        "backtestforecast.services.sweeps.get_thread_local_shared_execution_service",
        lambda: shared_execution_service,
    )
    monkeypatch.setattr(
        "backtestforecast.services.multi_symbol_backtests.get_thread_local_shared_execution_service",
        lambda: shared_execution_service,
    )
    monkeypatch.setattr(
        "backtestforecast.services.multi_step_backtests.get_thread_local_shared_execution_service",
        lambda: shared_execution_service,
    )

    backtest_service = BacktestService(MagicMock())
    scan_service = ScanService(MagicMock())
    sweep_service = SweepService(MagicMock())
    multi_symbol_service = MultiSymbolBacktestService(MagicMock())
    multi_step_service = MultiStepBacktestService(MagicMock())

    assert backtest_service.execution_service is shared_execution_service
    assert scan_service.execution_service is shared_execution_service
    assert sweep_service.execution_service is shared_execution_service
    assert multi_symbol_service.execution_service is shared_execution_service
    assert multi_step_service.execution_service is shared_execution_service

    backtest_service.close()
    scan_service.close()
    sweep_service.close()
    multi_symbol_service.close()
    multi_step_service.close()

    shared_execution_service.close.assert_not_called()
