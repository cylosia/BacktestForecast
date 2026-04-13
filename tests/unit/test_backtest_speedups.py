from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import backtestforecast.backtests.strategies.calendar as calendar_module
import backtestforecast.backtests.strategies.common as common_module
import backtestforecast.backtests.native_kernels as native_kernels_module
from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.strategies.common import (
    build_contract_delta_lookup,
    choose_primary_expiration_date,
    common_sorted_expirations,
    maybe_build_contract_delta_lookup,
    preferred_expiration_dates,
    require_contract_for_strike,
    resolve_strike,
    select_preferred_common_expiration_contracts,
    select_preferred_expiration_contracts,
)
from backtestforecast.backtests.strategies.calendar import resolve_calendar_contract_groups
from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import (
    BacktestConfig,
    BacktestExecutionResult,
    EquityPointResult,
    OpenMultiLegPosition,
    OpenOptionLeg,
    OpenStockLeg,
)
from backtestforecast.market_data.types import OptionContractRecord, OptionQuoteRecord
from backtestforecast.schemas.backtests import ComparisonOperator, RsiRule, StrikeSelection, StrikeSelectionMode
from backtestforecast.domain.execution_parameters import ResolvedExecutionParameters
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
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


class _PreparingStubMarketDataService:
    def __init__(self) -> None:
        self.client = SimpleNamespace(close=lambda: None)
        self._redis_cache = None
        self.prepare_call_count = 0

    def prepare_backtest(self, request: CreateBacktestRunRequest) -> HistoricalDataBundle:
        self.prepare_call_count += 1
        gateway = SimpleNamespace(gateway_id=self.prepare_call_count)
        bars = []
        current = request.start_date - timedelta(days=120)
        end = request.end_date + timedelta(days=180)
        while current <= end:
            bars.append(SimpleNamespace(trade_date=current))
            current += timedelta(days=1)
        return HistoricalDataBundle(
            bars=bars,
            earnings_dates=set(),
            ex_dividend_dates=set(),
            option_gateway=gateway,
        )


class _CapturingEngine:
    def __init__(self) -> None:
        self.shared_entry_rule_cache = None
        self.exit_variant_configs = None

    def run(self, *, config, bars, earnings_dates, ex_dividend_dates, option_gateway, shared_entry_rule_cache=None):
        self.shared_entry_rule_cache = shared_entry_rule_cache
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

    def run_exit_policy_variants(
        self,
        *,
        configs,
        bars,
        earnings_dates,
        ex_dividend_dates,
        option_gateway,
        shared_entry_rule_cache=None,
    ):
        self.shared_entry_rule_cache = shared_entry_rule_cache
        self.exit_variant_configs = list(configs)
        results = []
        for idx, config in enumerate(configs, 1):
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
                    equity=Decimal(str(10000 + (idx * 100))),
                    cash=Decimal(str(10000 + (idx * 100))),
                    position_value=Decimal("0"),
                    drawdown_pct=Decimal("0"),
                ),
            ]
            summary = build_summary(
                10000.0,
                float(curve[-1].equity),
                [],
                curve,
                risk_free_rate=config.risk_free_rate,
            )
            results.append(BacktestExecutionResult(summary=summary, trades=[], equity_curve=curve, warnings=[]))
        return results


class _RecordingExecutionEngine:
    def __init__(self) -> None:
        self.option_gateways: list[object] = []
        self.shared_entry_rule_caches: list[object] = []
        self.bars_counts: list[int] = []

    def run(self, *, config, bars, earnings_dates, ex_dividend_dates, option_gateway, shared_entry_rule_cache=None):
        self.option_gateways.append(option_gateway)
        self.shared_entry_rule_caches.append(shared_entry_rule_cache)
        self.bars_counts.append(len(bars))
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
        entry_trade_bars=None,
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


def test_historical_data_bundle_clone_shares_prefetch_state() -> None:
    bundle = HistoricalDataBundle(
        bars=[SimpleNamespace(trade_date=date(2025, 4, 1))],
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=SimpleNamespace(),
    )
    clone = bundle.clone_for_execution()
    signature = ("targeted_exact_quotes", "AAPL")

    bundle.remember_prefetch(signature, {"dates_processed": 2, "contracts_fetched": 4})

    assert clone.has_prefetched(signature) is True
    assert clone.get_prefetch_summary(signature) == {"dates_processed": 2, "contracts_fetched": 4}

    clone_summary = clone.get_prefetch_summary(signature)
    assert clone_summary is not None
    clone_summary["dates_processed"] = 999
    assert bundle.get_prefetch_summary(signature) == {"dates_processed": 2, "contracts_fetched": 4}


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


def test_butterfly_strategy_uses_batch_entry_quote_fetch_when_available() -> None:
    from backtestforecast.backtests.strategies.butterfly import ButterflyStrategy

    strategy = ButterflyStrategy()
    expiration = date(2025, 4, 18)
    lower_contract = OptionContractRecord("O:LOWER", "call", expiration, 95.0, 100.0)
    center_contract = OptionContractRecord("O:CENTER", "call", expiration, 100.0, 100.0)
    upper_contract = OptionContractRecord("O:UPPER", "call", expiration, 105.0, 100.0)
    config = BacktestConfig(
        symbol="AAPL",
        strategy_type="butterfly",
        start_date=date(2025, 4, 1),
        end_date=date(2025, 4, 30),
        target_dte=17,
        dte_tolerance_days=3,
        max_holding_days=10,
        account_size=10_000,
        risk_per_trade_pct=5,
        commission_per_contract=0,
        entry_rules=[],
    )
    bar = SimpleNamespace(trade_date=date(2025, 4, 1), close_price=100.0)

    class _Gateway:
        def list_contracts_for_preferred_expiration(self, **kwargs):
            return [lower_contract, center_contract, upper_contract]

        def get_quotes(self, option_tickers, trade_date):
            assert option_tickers == ["O:LOWER", "O:CENTER", "O:UPPER"]
            return {
                "O:LOWER": OptionQuoteRecord(trade_date, 4.9, 5.1, None),
                "O:CENTER": OptionQuoteRecord(trade_date, 2.9, 3.1, None),
                "O:UPPER": OptionQuoteRecord(trade_date, 0.9, 1.1, None),
            }

        def get_quote(self, option_ticker, trade_date):
            raise AssertionError("single-ticker quote path should not be used")

    position = strategy.build_position(config, bar, 0, _Gateway())

    assert position is not None
    assert [leg.ticker for leg in position.option_legs] == ["O:LOWER", "O:CENTER", "O:UPPER"]


def test_engine_mark_position_uses_attached_quote_series_before_gateway_fetch() -> None:
    engine = OptionsBacktestEngine()
    trade_date = date(2025, 4, 2)
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
        quote_series_lookup={
            "O:LEG1": {trade_date: OptionQuoteRecord(trade_date, 1.2, 1.4, None)},
            "O:LEG2": {trade_date: OptionQuoteRecord(trade_date, 0.6, 0.8, None)},
        },
        quote_series_loaded_tickers={"O:LEG1", "O:LEG2"},
    )
    bar = SimpleNamespace(trade_date=trade_date, close_price=102.0)

    class _Gateway:
        def get_quotes(self, option_tickers, trade_date):
            raise AssertionError("batch fetch should not run when quote series is attached")

        def get_quote(self, option_ticker, trade_date):
            raise AssertionError("single fetch should not run when quote series is attached")

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
    assert logged["prepared_bundle_cache_hit"] is False
    assert logged["data_source"] == "historical_flatfile"
    assert logged["prepare_ms"] == 2.0
    assert logged["prefetch_ms"] == 2.0
    assert logged["parameter_ms"] == 3.0
    assert logged["engine_ms"] == 8.0
    assert logged["staleness_ms"] == 1.0
    assert logged["total_ms"] == 22.0


def test_execution_service_reuses_internal_prepared_bundle_cache_for_narrower_requests(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    settings = SimpleNamespace(
        option_cache_warn_age_seconds=259_200,
        backtest_option_prefetch_enabled=False,
        backtest_prefetch_min_trade_dates=2,
        backtest_prefetch_max_dates=4,
        backtest_prefetch_timeout_seconds=77,
    )

    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, "build_backtest_risk_free_rate_curve", lambda *args, **kwargs: None)

    market_data_service = _PreparingStubMarketDataService()
    engine = _RecordingExecutionEngine()
    service = BacktestExecutionService(
        market_data_service=market_data_service,
        engine=engine,
    )
    first_request = CreateBacktestRunRequest(
        symbol="SPY",
        strategy_type="long_call",
        start_date="2025-01-02",
        end_date="2025-03-31",
        target_dte=60,
        dte_tolerance_days=10,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[
            {"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 21},
            {"type": "avoid_earnings", "days_before": 5, "days_after": 3},
        ],
    )
    second_request = CreateBacktestRunRequest(
        symbol="SPY",
        strategy_type="long_put",
        start_date="2025-01-02",
        end_date="2025-03-31",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[
            {"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14},
            {"type": "avoid_earnings", "days_before": 2, "days_after": 1},
        ],
    )
    resolved = ResolvedExecutionParameters(
        risk_free_rate=0.01,
        risk_free_rate_source="configured_fallback",
        risk_free_rate_field_name="yield_3_month",
        risk_free_rate_model="curve_default",
        dividend_yield=0.0,
        source_of_truth="test",
    )

    service.execute_request(first_request, resolved_parameters=resolved)
    service.execute_request(second_request, resolved_parameters=resolved)

    assert market_data_service.prepare_call_count == 1
    assert len(engine.option_gateways) == 2
    assert engine.option_gateways[0] is engine.option_gateways[1]
    assert len(engine.shared_entry_rule_caches) == 2
    assert engine.shared_entry_rule_caches[0] is not engine.shared_entry_rule_caches[1]
    assert len(engine.bars_counts) == 2
    assert engine.bars_counts[1] < engine.bars_counts[0]


def test_execution_service_rebuilds_internal_prepared_bundle_when_coverage_grows(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    settings = SimpleNamespace(
        option_cache_warn_age_seconds=259_200,
        backtest_option_prefetch_enabled=False,
        backtest_prefetch_min_trade_dates=2,
        backtest_prefetch_max_dates=4,
        backtest_prefetch_timeout_seconds=77,
    )

    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, "build_backtest_risk_free_rate_curve", lambda *args, **kwargs: None)

    market_data_service = _PreparingStubMarketDataService()
    service = BacktestExecutionService(
        market_data_service=market_data_service,
        engine=_CapturingEngine(),
    )
    first_request = CreateBacktestRunRequest(
        symbol="SPY",
        strategy_type="long_call",
        start_date="2025-01-02",
        end_date="2025-03-31",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[
            {"type": "avoid_earnings", "days_before": 1, "days_after": 1},
        ],
    )
    second_request = CreateBacktestRunRequest(
        symbol="SPY",
        strategy_type="long_put",
        start_date="2025-01-02",
        end_date="2025-03-31",
        target_dte=75,
        dte_tolerance_days=10,
        max_holding_days=30,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[
            {"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 21},
            {"type": "avoid_earnings", "days_before": 5, "days_after": 3},
        ],
    )
    resolved = ResolvedExecutionParameters(
        risk_free_rate=0.01,
        risk_free_rate_source="configured_fallback",
        risk_free_rate_field_name="yield_3_month",
        risk_free_rate_model="curve_default",
        dividend_yield=0.0,
        source_of_truth="test",
    )

    service.execute_request(first_request, resolved_parameters=resolved)
    service.execute_request(second_request, resolved_parameters=resolved)

    assert market_data_service.prepare_call_count == 2


def test_execution_service_deduplicates_inflight_prefetch_across_bundle_clones(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    bundle = HistoricalDataBundle(
        bars=[SimpleNamespace(trade_date=date(2025, 4, 1))],
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=SimpleNamespace(),
    )
    clone = bundle.clone_for_execution()
    service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle),
        engine=_CapturingEngine(),
    )
    plan = module._PrefetchPlan(
        mode="targeted_strategy_exact_contracts",
        signature=("prefetch", "AAPL"),
        include_quotes=False,
        warm_future_quotes=False,
        max_dates=1,
        trade_dates=(date(2025, 4, 1),),
    )
    started = threading.Event()
    release = threading.Event()
    call_count = 0

    monkeypatch.setattr(service, "_build_prefetch_plan", lambda request, bundle, settings: plan)

    def _fake_run_prefetch_plan(request, bundle, plan, settings):
        nonlocal call_count
        call_count += 1
        started.set()
        assert release.wait(timeout=2)
        return module._PrefetchResult(
            mode=plan.mode,
            summary={"dates_processed": 1, "contracts_fetched": 5},
        )

    monkeypatch.setattr(service, "_run_prefetch_plan", _fake_run_prefetch_plan)

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
        entry_rules=[],
    )
    settings = SimpleNamespace(backtest_prefetch_timeout_seconds=5)
    results: list[object | None] = [None, None]

    def _run(index: int, execution_bundle: HistoricalDataBundle) -> None:
        results[index] = service._maybe_prefetch_option_data(request, execution_bundle, settings)

    thread_one = threading.Thread(target=_run, args=(0, bundle))
    thread_two = threading.Thread(target=_run, args=(1, clone))
    thread_one.start()
    assert started.wait(timeout=1)
    thread_two.start()
    release.set()
    thread_one.join(timeout=2)
    thread_two.join(timeout=2)

    assert call_count == 1
    assert sorted(result.skipped for result in results if result is not None) == [False, True]


def test_execution_service_caches_resolved_execution_inputs(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    settings = SimpleNamespace(
        option_cache_warn_age_seconds=259_200,
        backtest_option_prefetch_enabled=False,
        backtest_prefetch_min_trade_dates=2,
        backtest_prefetch_max_dates=4,
        backtest_prefetch_timeout_seconds=77,
    )
    resolve_calls = {"rate": 0, "curve": 0}

    def _fake_resolve_rate(request, *, client=None):
        resolve_calls["rate"] += 1
        return SimpleNamespace(rate=0.01, source="configured_fallback", field_name="yield_3_month")

    def _fake_build_curve(request, *, default_rate, client=None):
        resolve_calls["curve"] += 1
        return SimpleNamespace(default_rate=default_rate, dates=(), rates=())

    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, "resolve_backtest_risk_free_rate", _fake_resolve_rate)
    monkeypatch.setattr(module, "build_backtest_risk_free_rate_curve", _fake_build_curve)

    bars = [
        SimpleNamespace(trade_date=date(2025, 4, 1)),
        SimpleNamespace(trade_date=date(2025, 4, 2)),
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
        end_date="2025-04-02",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[],
    )

    first = service.execute_request(request, bundle=bundle)
    second = service.execute_request(request, bundle=bundle)

    assert first.summary.ending_equity == 10100.0
    assert second.summary.ending_equity == 10100.0
    assert resolve_calls == {"rate": 1, "curve": 1}


def test_select_preferred_expiration_contracts_reuses_selection_cache() -> None:
    calls: list[tuple[date, str, int, int]] = []
    contract = OptionContractRecord(
        ticker="O:SPY250425C00500000",
        contract_type="call",
        expiration_date=date(2025, 4, 25),
        strike_price=500.0,
        shares_per_contract=100.0,
    )

    class _Gateway:
        def list_contracts_for_preferred_expiration(self, **kwargs):
            calls.append(
                (
                    kwargs["entry_date"],
                    kwargs["contract_type"],
                    kwargs["target_dte"],
                    kwargs["dte_tolerance_days"],
                )
            )
            return [contract]

    gateway = _Gateway()

    first = select_preferred_expiration_contracts(
        gateway,
        entry_date=date(2025, 4, 1),
        contract_type="call",
        target_dte=24,
        dte_tolerance_days=5,
    )
    second = select_preferred_expiration_contracts(
        gateway,
        entry_date=date(2025, 4, 1),
        contract_type="call",
        target_dte=24,
        dte_tolerance_days=5,
    )

    assert calls == [(date(2025, 4, 1), "call", 24, 5)]
    assert first == second


def test_select_preferred_common_expiration_contracts_reuses_selection_cache() -> None:
    call_batches = 0
    put_batches = 0
    expiration = date(2025, 4, 25)
    call_contract = OptionContractRecord(
        ticker="O:SPY250425C00500000",
        contract_type="call",
        expiration_date=expiration,
        strike_price=500.0,
        shares_per_contract=100.0,
    )
    put_contract = OptionContractRecord(
        ticker="O:SPY250425P00500000",
        contract_type="put",
        expiration_date=expiration,
        strike_price=500.0,
        shares_per_contract=100.0,
    )

    class _Gateway:
        def list_contracts_for_expirations(self, **kwargs):
            nonlocal call_batches, put_batches
            if kwargs["contract_type"] == "call":
                call_batches += 1
                return {expiration: [call_contract]}
            put_batches += 1
            return {expiration: [put_contract]}

    gateway = _Gateway()

    first = select_preferred_common_expiration_contracts(
        gateway,
        entry_date=date(2025, 4, 1),
        target_dte=24,
        dte_tolerance_days=5,
    )
    second = select_preferred_common_expiration_contracts(
        gateway,
        entry_date=date(2025, 4, 1),
        target_dte=24,
        dte_tolerance_days=5,
    )

    assert call_batches == 1
    assert put_batches == 1
    assert first == second


def test_resolve_calendar_contract_groups_reuses_selection_cache() -> None:
    calls: list[date] = []

    def _contracts_for(expiration_date: date) -> list[OptionContractRecord]:
        return [
            OptionContractRecord(
                ticker=f"O:SPY{expiration_date.strftime('%y%m%d')}C00500000",
                contract_type="call",
                expiration_date=expiration_date,
                strike_price=500.0,
                shares_per_contract=100.0,
            )
        ]

    class _Gateway:
        def list_contracts_for_expiration(self, **kwargs):
            expiration_date = kwargs["expiration_date"]
            calls.append(expiration_date)
            if expiration_date in {date(2025, 4, 8), date(2025, 4, 9)}:
                return _contracts_for(expiration_date)
            return []

    gateway = _Gateway()

    first = resolve_calendar_contract_groups(
        gateway,
        entry_date=date(2025, 4, 1),
        contract_type="call",
        target_dte=7,
        dte_tolerance_days=1,
    )
    second = resolve_calendar_contract_groups(
        gateway,
        entry_date=date(2025, 4, 1),
        contract_type="call",
        target_dte=7,
        dte_tolerance_days=1,
    )

    assert calls == [date(2025, 4, 8), date(2025, 4, 9)]
    assert first == second


def test_delta_lookup_uses_contract_as_of_mid_price_without_gateway_quote_fetch(monkeypatch) -> None:
    import backtestforecast.backtests.rules as rules_module

    class _Clock:
        def __init__(self) -> None:
            self.value = 0.0

        def perf_counter(self) -> float:
            return self.value

        def advance(self, seconds: float) -> None:
            self.value += seconds

    class _Gateway:
        def __init__(self) -> None:
            self.iv_cache: dict[tuple[str, date], float | None] = {}

        def get_iv(self, key):
            return key in self.iv_cache, self.iv_cache.get(key)

        def store_iv(self, key, value):
            self.iv_cache[key] = value

        def get_quote(self, option_ticker, trade_date):
            raise AssertionError("contract as_of_mid_price should avoid quote fetch")

        def get_quotes(self, option_tickers, trade_date):
            raise AssertionError("contract as_of_mid_price should avoid batch quote fetch")

    clock = _Clock()
    monkeypatch.setattr(common_module._time, "perf_counter", clock.perf_counter)

    def _fake_implied_volatility_from_price(**kwargs):
        clock.advance(0.006)
        return 0.22

    def _fake_approx_bsm_delta_many(*args, **kwargs):
        clock.advance(0.008)
        return [0.31]

    monkeypatch.setattr(rules_module, "implied_volatility_from_price", _fake_implied_volatility_from_price)
    monkeypatch.setattr(common_module, "_kernel_approx_bsm_delta_many", _fake_approx_bsm_delta_many)

    selection = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))
    contracts = [
        OptionContractRecord(
            "O:AAPL250502C00100000",
            "call",
            date(2025, 5, 2),
            100.0,
            100.0,
            as_of_mid_price=2.0,
        )
    ]
    gateway = _Gateway()

    with common_module._DELTA_LOOKUP_CACHE_LOCK:
        common_module._DELTA_LOOKUP_CACHE.clear()

    profiler = common_module.BuildPositionProfiler()
    token = common_module.activate_build_position_profiler(profiler)
    try:
        lookup = maybe_build_contract_delta_lookup(
            selection=selection,
            contracts=contracts,
            option_gateway=gateway,
            trade_date=date(2025, 4, 1),
            underlying_close=100.0,
            dte_days=31,
        )
    finally:
        common_module.reset_build_position_profiler(token)
        with common_module._DELTA_LOOKUP_CACHE_LOCK:
            common_module._DELTA_LOOKUP_CACHE.clear()

    assert lookup == {(100.0, date(2025, 5, 2)): 0.31}
    assert profiler.delta_iv_quote_fetch_ms == pytest.approx(0.0)
    assert profiler.delta_iv_solve_ms == pytest.approx(6.0)
    assert profiler.delta_kernel_ms == pytest.approx(8.0)


def test_get_entry_quotes_prefers_contract_as_of_mid_price() -> None:
    contracts = [
        OptionContractRecord(
            "O:AAPL250502C00100000",
            "call",
            date(2025, 5, 2),
            100.0,
            100.0,
            as_of_mid_price=2.0,
        ),
        OptionContractRecord(
            "O:AAPL250502C00105000",
            "call",
            date(2025, 5, 2),
            105.0,
            100.0,
            as_of_mid_price=1.0,
        ),
    ]

    class _Gateway:
        def get_quotes(self, option_tickers, trade_date):
            raise AssertionError("contract as_of_mid_price should avoid batch quote fetch")

        def get_quote(self, option_ticker, trade_date):
            raise AssertionError("contract as_of_mid_price should avoid quote fetch")

    quotes = common_module.get_entry_quotes(
        _Gateway(),
        trade_date=date(2025, 4, 1),
        contracts=contracts,
    )

    assert quotes["O:AAPL250502C00100000"].mid_price == 2.0
    assert quotes["O:AAPL250502C00105000"].mid_price == 1.0


def test_engine_enrich_position_option_legs_skips_quote_fetch_when_metadata_seeded() -> None:
    engine = OptionsBacktestEngine()
    position = OpenMultiLegPosition(
        display_ticker="O:AAPL250502C00100000",
        strategy_type="calendar_spread",
        underlying_symbol="AAPL",
        entry_date=date(2025, 4, 1),
        entry_index=0,
        quantity=1,
        dte_at_open=31,
        option_legs=[
            OpenOptionLeg(
                ticker="O:AAPL250502C00100000",
                contract_type="call",
                side=1,
                strike_price=100.0,
                expiration_date=date(2025, 5, 2),
                quantity_per_unit=1,
                entry_mid=2.0,
                last_mid=2.0,
                deliverable_shares_per_contract=100.0,
                contract_root_symbol="AAPL",
                reference_underlying_symbol="AAPL",
            )
        ],
    )

    class _Gateway:
        def get_quotes(self, option_tickers, trade_date):
            raise AssertionError("pre-seeded metadata should avoid enrichment quote fetch")

    engine._enrich_position_option_legs(position=position, option_gateway=_Gateway())

    assert position.option_legs[0].contract_root_symbol == "AAPL"
    assert position.option_legs[0].reference_underlying_symbol == "AAPL"
    assert position.option_legs[0].is_nonstandard is False


def test_engine_run_logs_phase_timing_breakdown(monkeypatch) -> None:
    import backtestforecast.backtests.engine as engine_module

    logged: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(
        engine_module,
        "logger",
        SimpleNamespace(
            info=lambda event, **kwargs: logged.append((event, kwargs)),
            warning=lambda *args, **kwargs: None,
            debug=lambda *args, **kwargs: None,
        ),
    )

    contract = OptionContractRecord("C100", "call", date(2025, 10, 17), 100.0, 100.0)
    bars = [
        SimpleNamespace(trade_date=date(2025, 9, 2), open_price=100.0, high_price=100.0, low_price=100.0, close_price=100.0, volume=1_000_000),
        SimpleNamespace(trade_date=date(2025, 9, 3), open_price=101.0, high_price=101.0, low_price=101.0, close_price=101.0, volume=1_000_000),
    ]

    class _Gateway:
        def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
            assert contract_type == "call"
            return [contract]

        def get_quote(self, option_ticker, trade_date):
            prices = {
                date(2025, 9, 2): 2.0,
                date(2025, 9, 3): 3.2,
            }
            price = prices.get(trade_date)
            if price is None:
                return None
            return OptionQuoteRecord(trade_date=trade_date, bid_price=price - 0.1, ask_price=price + 0.1, participant_timestamp=None)

        def get_ex_dividend_dates(self, start_date, end_date):
            return set()

    result = OptionsBacktestEngine().run(
        BacktestConfig(
            symbol="AAPL",
            strategy_type="long_call",
            start_date=date(2025, 9, 2),
            end_date=date(2025, 9, 2),
            target_dte=30,
            dte_tolerance_days=30,
            max_holding_days=1,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("5"),
            commission_per_contract=Decimal("0.65"),
            entry_rules=[],
        ),
        bars,
        set(),
        _Gateway(),
    )

    assert result.summary.trade_count == 1
    event_name, payload = logged[-1]
    assert event_name == "backtest.engine_run_timing"
    assert payload["bars_processed"] == 2
    assert payload["positions_opened"] == 1
    assert payload["positions_closed"] == 1
    assert payload["mark_position_ms"] >= 0.0
    assert payload["exit_resolution_ms"] >= 0.0
    assert payload["build_position_ms"] >= 0.0
    assert payload["build_contract_fetch_ms"] >= 0.0
    assert payload["build_contract_selector_fetch_ms"] >= 0.0
    assert payload["build_contract_availability_fetch_ms"] >= 0.0
    assert payload["build_contract_batch_fetch_ms"] >= 0.0
    assert payload["build_contract_exact_fetch_ms"] >= 0.0
    assert payload["build_contract_other_ms"] >= 0.0
    assert payload["build_contract_selection_cache_hits"] >= 0
    assert payload["build_contract_selection_cache_misses"] >= 0
    assert isinstance(payload["build_contract_gateway_method_ms"], dict)
    assert isinstance(payload["build_contract_gateway_method_calls"], dict)
    assert payload["build_contract_gateway_contract_cache_hits"] >= 0
    assert payload["build_contract_gateway_contract_cache_misses"] >= 0
    assert payload["build_contract_gateway_exact_cache_hits"] >= 0
    assert payload["build_contract_gateway_exact_cache_misses"] >= 0
    assert payload["build_contract_gateway_availability_cache_hits"] >= 0
    assert payload["build_contract_gateway_availability_cache_misses"] >= 0
    assert payload["build_contract_gateway_availability_by_type_cache_hits"] >= 0
    assert payload["build_contract_gateway_availability_by_type_cache_misses"] >= 0
    assert payload["build_delta_resolution_ms"] >= 0.0
    assert payload["build_delta_iv_quote_fetch_ms"] >= 0.0
    assert payload["build_delta_iv_solve_ms"] >= 0.0
    assert payload["build_delta_kernel_ms"] >= 0.0
    assert payload["build_delta_other_ms"] >= 0.0
    assert payload["build_delta_lookup_cache_hits"] >= 0
    assert payload["build_delta_lookup_cache_misses"] >= 0
    assert payload["build_delta_iv_cache_hits"] >= 0
    assert payload["build_delta_iv_cache_misses"] >= 0
    assert payload["build_entry_quote_fetch_ms"] >= 0.0
    assert payload["build_object_construction_ms"] >= 0.0
    assert payload["attach_quote_series_ms"] >= 0.0
    assert payload["position_sizing_ms"] >= 0.0
    assert payload["close_position_ms"] >= 0.0
    assert payload["summary_ms"] >= 0.0
    assert payload["total_ms"] >= payload["summary_ms"]


def test_engine_run_exit_policy_variants_logs_phase_timing_breakdown(monkeypatch) -> None:
    import backtestforecast.backtests.engine as engine_module

    logged: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(
        engine_module,
        "logger",
        SimpleNamespace(
            info=lambda event, **kwargs: logged.append((event, kwargs)),
            warning=lambda *args, **kwargs: None,
            debug=lambda *args, **kwargs: None,
        ),
    )

    contract = OptionContractRecord("C100", "call", date(2025, 10, 17), 100.0, 100.0)
    bars = [
        SimpleNamespace(trade_date=date(2025, 9, 2), open_price=100.0, high_price=100.0, low_price=100.0, close_price=100.0, volume=1_000_000),
        SimpleNamespace(trade_date=date(2025, 9, 3), open_price=101.0, high_price=101.0, low_price=101.0, close_price=101.0, volume=1_000_000),
        SimpleNamespace(trade_date=date(2025, 9, 4), open_price=102.0, high_price=102.0, low_price=102.0, close_price=102.0, volume=1_000_000),
    ]

    class _Gateway:
        def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
            assert contract_type == "call"
            return [contract]

        def get_quote(self, option_ticker, trade_date):
            prices = {
                date(2025, 9, 2): 2.0,
                date(2025, 9, 3): 3.2,
                date(2025, 9, 4): 3.7,
            }
            price = prices.get(trade_date)
            if price is None:
                return None
            return OptionQuoteRecord(trade_date=trade_date, bid_price=price - 0.1, ask_price=price + 0.1, participant_timestamp=None)

        def get_ex_dividend_dates(self, start_date, end_date):
            return set()

    base_kwargs = dict(
        symbol="AAPL",
        strategy_type="long_call",
        start_date=date(2025, 9, 2),
        end_date=date(2025, 9, 2),
        target_dte=30,
        dte_tolerance_days=30,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        entry_rules=[],
    )
    results = OptionsBacktestEngine().run_exit_policy_variants(
        configs=[
            BacktestConfig(**base_kwargs, profit_target_pct=50),
            BacktestConfig(**base_kwargs, profit_target_pct=75),
        ],
        bars=bars,
        earnings_dates=set(),
        option_gateway=_Gateway(),
    )

    assert len(results) == 2
    event_name, payload = logged[-1]
    assert event_name == "backtest.engine_exit_variants_timing"
    assert payload["lane_count"] == 2
    assert payload["positions_opened"] == 2
    assert payload["positions_closed"] == 2
    assert payload["rule_precompute_ms"] >= 0.0
    assert payload["mark_position_ms"] >= 0.0
    assert payload["build_position_ms"] >= 0.0
    assert payload["build_contract_fetch_ms"] >= 0.0
    assert payload["build_contract_selector_fetch_ms"] >= 0.0
    assert payload["build_contract_availability_fetch_ms"] >= 0.0
    assert payload["build_contract_batch_fetch_ms"] >= 0.0
    assert payload["build_contract_exact_fetch_ms"] >= 0.0
    assert payload["build_contract_other_ms"] >= 0.0
    assert payload["build_contract_selection_cache_hits"] >= 0
    assert payload["build_contract_selection_cache_misses"] >= 0
    assert isinstance(payload["build_contract_gateway_method_ms"], dict)
    assert isinstance(payload["build_contract_gateway_method_calls"], dict)
    assert payload["build_delta_resolution_ms"] >= 0.0
    assert payload["build_delta_iv_quote_fetch_ms"] >= 0.0
    assert payload["build_delta_iv_solve_ms"] >= 0.0
    assert payload["build_delta_kernel_ms"] >= 0.0
    assert payload["build_delta_other_ms"] >= 0.0
    assert payload["build_delta_lookup_cache_hits"] >= 0
    assert payload["build_delta_lookup_cache_misses"] >= 0
    assert payload["build_delta_iv_cache_hits"] >= 0
    assert payload["build_delta_iv_cache_misses"] >= 0
    assert payload["build_entry_quote_fetch_ms"] >= 0.0
    assert payload["build_object_construction_ms"] >= 0.0
    assert payload["attach_quote_series_ms"] >= 0.0
    assert payload["position_sizing_ms"] >= 0.0
    assert payload["equity_curve_ms"] >= 0.0
    assert payload["summary_ms"] >= 0.0
    assert payload["total_ms"] >= payload["summary_ms"]


def test_engine_run_defers_quote_series_attachment_until_after_affordability(monkeypatch) -> None:
    engine = OptionsBacktestEngine()
    attach_calls: list[tuple[date, date]] = []

    monkeypatch.setattr(
        engine,
        "_attach_position_quote_series",
        lambda position, *, option_gateway, start_date, end_date: attach_calls.append((start_date, end_date)),
    )
    monkeypatch.setattr(engine, "_resolve_position_size", lambda **kwargs: 0)

    contract = OptionContractRecord("C100", "call", date(2025, 10, 17), 100.0, 100.0)
    bars = [
        SimpleNamespace(
            trade_date=date(2025, 9, 2),
            open_price=100.0,
            high_price=100.0,
            low_price=100.0,
            close_price=100.0,
            volume=1_000_000,
        ),
    ]

    class _Gateway:
        def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
            return [contract]

        def get_quote(self, option_ticker, trade_date):
            return OptionQuoteRecord(
                trade_date=trade_date,
                bid_price=1.9,
                ask_price=2.1,
                participant_timestamp=None,
            )

        def get_ex_dividend_dates(self, start_date, end_date):
            return set()

    result = engine.run(
        BacktestConfig(
            symbol="AAPL",
            strategy_type="long_call",
            start_date=date(2025, 9, 2),
            end_date=date(2025, 9, 2),
            target_dte=30,
            dte_tolerance_days=30,
            max_holding_days=1,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("5"),
            commission_per_contract=Decimal("0.65"),
            entry_rules=[],
        ),
        bars,
        set(),
        _Gateway(),
    )

    assert result.summary.trade_count == 0
    assert attach_calls == []


def test_engine_run_exit_policy_variants_attaches_quote_series_only_after_viable_lane(monkeypatch) -> None:
    engine = OptionsBacktestEngine()
    attach_calls: list[tuple[date, date]] = []
    sizing_results = iter([0, 1])

    monkeypatch.setattr(
        engine,
        "_attach_position_quote_series",
        lambda position, *, option_gateway, start_date, end_date: attach_calls.append((start_date, end_date)),
    )
    monkeypatch.setattr(engine, "_resolve_position_size", lambda **kwargs: next(sizing_results))

    contract = OptionContractRecord("C100", "call", date(2025, 10, 17), 100.0, 100.0)
    bars = [
        SimpleNamespace(
            trade_date=date(2025, 9, 2),
            open_price=100.0,
            high_price=100.0,
            low_price=100.0,
            close_price=100.0,
            volume=1_000_000,
        ),
        SimpleNamespace(
            trade_date=date(2025, 9, 3),
            open_price=101.0,
            high_price=101.0,
            low_price=101.0,
            close_price=101.0,
            volume=1_000_000,
        ),
    ]

    class _Gateway:
        def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
            return [contract]

        def get_quote(self, option_ticker, trade_date):
            price = 2.0 if trade_date == date(2025, 9, 2) else 2.6
            return OptionQuoteRecord(
                trade_date=trade_date,
                bid_price=price - 0.1,
                ask_price=price + 0.1,
                participant_timestamp=None,
            )

        def get_ex_dividend_dates(self, start_date, end_date):
            return set()

    base_kwargs = dict(
        symbol="AAPL",
        strategy_type="long_call",
        start_date=date(2025, 9, 2),
        end_date=date(2025, 9, 2),
        target_dte=30,
        dte_tolerance_days=30,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        entry_rules=[],
    )
    results = engine.run_exit_policy_variants(
        configs=[
            BacktestConfig(**base_kwargs, profit_target_pct=50),
            BacktestConfig(**base_kwargs, profit_target_pct=75),
        ],
        bars=bars,
        earnings_dates=set(),
        option_gateway=_Gateway(),
    )

    assert len(results) == 2
    assert attach_calls == [(date(2025, 9, 2), date(2025, 9, 3))]
    assert [result.summary.trade_count for result in results] == [0, 1]


def test_attach_position_quote_series_reuses_gateway_dict_payload() -> None:
    quote_series = {
        "O:LEG1": {
            date(2025, 9, 2): OptionQuoteRecord(date(2025, 9, 2), 1.9, 2.1, None),
            date(2025, 9, 3): OptionQuoteRecord(date(2025, 9, 3), 2.4, 2.6, None),
        },
    }

    class _Gateway:
        def get_quote_series(self, option_tickers, start_date, end_date):
            assert option_tickers == ["O:LEG1"]
            assert start_date == date(2025, 9, 2)
            assert end_date == date(2025, 9, 3)
            return quote_series

    position = OpenMultiLegPosition(
        display_ticker="O:LEG1",
        strategy_type="long_call",
        underlying_symbol="AAPL",
        entry_date=date(2025, 9, 2),
        entry_index=0,
        quantity=1,
        dte_at_open=10,
        option_legs=[
            OpenOptionLeg("O:LEG1", "call", 1, 100.0, date(2025, 9, 12), 1, 2.0, 2.0),
        ],
    )

    OptionsBacktestEngine._attach_position_quote_series(
        position,
        option_gateway=_Gateway(),
        start_date=date(2025, 9, 2),
        end_date=date(2025, 9, 3),
    )

    assert position.quote_series_lookup is quote_series
    assert position.quote_series_lookup["O:LEG1"] is quote_series["O:LEG1"]
    assert position.quote_series_loaded_tickers == {"O:LEG1"}


def test_delta_lookup_profiler_tracks_quote_iv_kernel_and_cache_metrics(monkeypatch) -> None:
    import backtestforecast.backtests.rules as rules_module

    class _Clock:
        def __init__(self) -> None:
            self.value = 0.0

        def perf_counter(self) -> float:
            return self.value

        def advance(self, seconds: float) -> None:
            self.value += seconds

    class _Gateway:
        def __init__(self) -> None:
            self.iv_cache: dict[tuple[str, date], float | None] = {}
            self.quote_calls: list[tuple[str, date]] = []

        def get_iv(self, key):
            return key in self.iv_cache, self.iv_cache.get(key)

        def store_iv(self, key, value):
            self.iv_cache[key] = value

        def get_quote(self, option_ticker, trade_date):
            self.quote_calls.append((option_ticker, trade_date))
            clock.advance(0.004)
            return OptionQuoteRecord(
                trade_date=trade_date,
                bid_price=1.9,
                ask_price=2.1,
                participant_timestamp=None,
            )

    clock = _Clock()
    monkeypatch.setattr(common_module._time, "perf_counter", clock.perf_counter)

    def _fake_implied_volatility_from_price(**kwargs):
        clock.advance(0.006)
        return 0.22

    def _fake_approx_bsm_delta_many(*args, **kwargs):
        clock.advance(0.008)
        return [0.31]

    monkeypatch.setattr(rules_module, "implied_volatility_from_price", _fake_implied_volatility_from_price)
    monkeypatch.setattr(common_module, "_kernel_approx_bsm_delta_many", _fake_approx_bsm_delta_many)

    selection = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))
    contracts = [OptionContractRecord("O:AAPL250502C00100000", "call", date(2025, 5, 2), 100.0, 100.0)]
    gateway = _Gateway()

    with common_module._DELTA_LOOKUP_CACHE_LOCK:
        common_module._DELTA_LOOKUP_CACHE.clear()

    profiler = common_module.BuildPositionProfiler()
    token = common_module.activate_build_position_profiler(profiler)
    try:
        first = maybe_build_contract_delta_lookup(
            selection=selection,
            contracts=contracts,
            option_gateway=gateway,
            trade_date=date(2025, 4, 1),
            underlying_close=100.0,
            dte_days=31,
        )
        second = maybe_build_contract_delta_lookup(
            selection=selection,
            contracts=contracts,
            option_gateway=gateway,
            trade_date=date(2025, 4, 1),
            underlying_close=101.0,
            dte_days=31,
        )
        third = maybe_build_contract_delta_lookup(
            selection=selection,
            contracts=contracts,
            option_gateway=gateway,
            trade_date=date(2025, 4, 1),
            underlying_close=101.0,
            dte_days=31,
        )
    finally:
        common_module.reset_build_position_profiler(token)
        with common_module._DELTA_LOOKUP_CACHE_LOCK:
            common_module._DELTA_LOOKUP_CACHE.clear()

    expected_lookup = {(100.0, date(2025, 5, 2)): 0.31}
    assert first == expected_lookup
    assert second == expected_lookup
    assert third == expected_lookup
    assert gateway.quote_calls == [("O:AAPL250502C00100000", date(2025, 4, 1))]
    assert profiler.delta_lookup_ms == pytest.approx(26.0)
    assert profiler.delta_iv_quote_fetch_ms == pytest.approx(4.0)
    assert profiler.delta_iv_solve_ms == pytest.approx(6.0)
    assert profiler.delta_kernel_ms == pytest.approx(16.0)
    assert profiler.delta_lookup_cache_hits == 1
    assert profiler.delta_lookup_cache_misses == 2
    assert profiler.delta_iv_cache_hits == 1
    assert profiler.delta_iv_cache_misses == 1


def test_contract_fetch_profiler_tracks_selector_availability_batch_and_cache_metrics(monkeypatch) -> None:
    class _Clock:
        def __init__(self) -> None:
            self.value = 0.0

        def perf_counter(self) -> float:
            return self.value

        def advance(self, seconds: float) -> None:
            self.value += seconds

    clock = _Clock()
    monkeypatch.setattr(common_module._time, "perf_counter", clock.perf_counter)

    expiration = date(2025, 4, 4)
    preferred_contract = OptionContractRecord("O:AAPL250404C00100000", "call", expiration, 100.0, 100.0)
    call_contract = OptionContractRecord("O:AAPL250404C00105000", "call", expiration, 105.0, 100.0)
    put_contract = OptionContractRecord("O:AAPL250404P00095000", "put", expiration, 95.0, 100.0)

    class _PreferredGateway:
        def list_contracts_for_preferred_expiration(self, **kwargs):
            clock.advance(0.002)
            return [preferred_contract]

    class _CommonGateway:
        def list_available_expirations_by_type(self, **kwargs):
            clock.advance(0.003)
            return {"call": [expiration], "put": [expiration]}

        def list_contracts_for_expirations_by_type(self, **kwargs):
            clock.advance(0.004)
            return {
                "call": {expiration: [call_contract]},
                "put": {expiration: [put_contract]},
            }

    preferred_gateway = _PreferredGateway()
    common_gateway = _CommonGateway()

    with common_module._PREFERRED_EXPIRATION_SELECTION_CACHE_LOCK:
        common_module._PREFERRED_EXPIRATION_SELECTION_CACHE.clear()
    with common_module._COMMON_EXPIRATION_SELECTION_CACHE_LOCK:
        common_module._COMMON_EXPIRATION_SELECTION_CACHE.clear()

    profiler = common_module.BuildPositionProfiler()
    token = common_module.activate_build_position_profiler(profiler)
    try:
        first_preferred = select_preferred_expiration_contracts(
            preferred_gateway,
            entry_date=date(2025, 4, 1),
            contract_type="call",
            target_dte=3,
            dte_tolerance_days=1,
        )
        second_preferred = select_preferred_expiration_contracts(
            preferred_gateway,
            entry_date=date(2025, 4, 1),
            contract_type="call",
            target_dte=3,
            dte_tolerance_days=1,
        )
        first_common = select_preferred_common_expiration_contracts(
            common_gateway,
            entry_date=date(2025, 4, 1),
            target_dte=3,
            dte_tolerance_days=1,
        )
        second_common = select_preferred_common_expiration_contracts(
            common_gateway,
            entry_date=date(2025, 4, 1),
            target_dte=3,
            dte_tolerance_days=1,
        )
    finally:
        common_module.reset_build_position_profiler(token)
        with common_module._PREFERRED_EXPIRATION_SELECTION_CACHE_LOCK:
            common_module._PREFERRED_EXPIRATION_SELECTION_CACHE.clear()
        with common_module._COMMON_EXPIRATION_SELECTION_CACHE_LOCK:
            common_module._COMMON_EXPIRATION_SELECTION_CACHE.clear()

    assert first_preferred == second_preferred
    assert first_common == second_common
    assert profiler.contract_fetch_ms == pytest.approx(6.0)
    assert profiler.contract_selector_fetch_ms == pytest.approx(2.0)
    assert profiler.contract_availability_fetch_ms == pytest.approx(0.0)
    assert profiler.contract_batch_fetch_ms == pytest.approx(4.0)
    assert profiler.contract_exact_fetch_ms == pytest.approx(0.0)
    assert profiler.contract_selection_cache_hits == 2
    assert profiler.contract_selection_cache_misses == 2


def test_contract_fetch_profiler_uses_specialized_common_selector_gateway(monkeypatch) -> None:
    class _Clock:
        def __init__(self) -> None:
            self.value = 0.0

        def perf_counter(self) -> float:
            return self.value

        def advance(self, seconds: float) -> None:
            self.value += seconds

    clock = _Clock()
    monkeypatch.setattr(common_module._time, "perf_counter", clock.perf_counter)

    expiration = date(2025, 4, 4)
    call_contract = OptionContractRecord("O:AAPL250404C00105000", "call", expiration, 105.0, 100.0)
    put_contract = OptionContractRecord("O:AAPL250404P00095000", "put", expiration, 95.0, 100.0)

    class _Gateway:
        def list_contracts_for_preferred_common_expiration(self, **kwargs):
            clock.advance(0.004)
            return expiration, [call_contract], [put_contract]

        def list_contracts_for_expirations_by_type(self, **kwargs):
            raise AssertionError("specialized common-expiration path should short-circuit generic batch fetch")

    gateway = _Gateway()

    with common_module._COMMON_EXPIRATION_SELECTION_CACHE_LOCK:
        common_module._COMMON_EXPIRATION_SELECTION_CACHE.clear()

    profiler = common_module.BuildPositionProfiler()
    token = common_module.activate_build_position_profiler(profiler)
    try:
        first = select_preferred_common_expiration_contracts(
            gateway,
            entry_date=date(2025, 4, 1),
            target_dte=3,
            dte_tolerance_days=1,
        )
        second = select_preferred_common_expiration_contracts(
            gateway,
            entry_date=date(2025, 4, 1),
            target_dte=3,
            dte_tolerance_days=1,
        )
    finally:
        common_module.reset_build_position_profiler(token)
        with common_module._COMMON_EXPIRATION_SELECTION_CACHE_LOCK:
            common_module._COMMON_EXPIRATION_SELECTION_CACHE.clear()

    assert first == second
    assert profiler.contract_fetch_ms == pytest.approx(4.0)
    assert profiler.contract_selector_fetch_ms == pytest.approx(4.0)
    assert profiler.contract_batch_fetch_ms == pytest.approx(0.0)
    assert profiler.contract_selection_cache_hits == 1
    assert profiler.contract_selection_cache_misses == 1


def test_timed_build_position_gateway_tracks_contract_gateway_methods(monkeypatch) -> None:
    import backtestforecast.backtests.engine as engine_module

    class _Clock:
        def __init__(self) -> None:
            self.value = 0.0

        def perf_counter(self) -> float:
            return self.value

        def advance(self, seconds: float) -> None:
            self.value += seconds

    clock = _Clock()
    monkeypatch.setattr(engine_module._time, "perf_counter", clock.perf_counter)

    class _Gateway:
        def list_contracts(self, entry_date, contract_type, target_dte, dte_tolerance_days):
            clock.advance(0.002)
            return []

        def list_contracts_for_preferred_common_expiration(self, **kwargs):
            clock.advance(0.004)
            return date(2025, 4, 4), [], []

        def list_available_expirations_by_type(self, **kwargs):
            clock.advance(0.003)
            return {"call": [], "put": []}

    profiler = common_module.BuildPositionProfiler()
    wrapped = engine_module._TimedBuildPositionGateway(_Gateway(), profiler)

    wrapped.list_contracts(date(2025, 4, 1), "call", 30, 5)
    wrapped.list_contracts_for_preferred_common_expiration(
        entry_date=date(2025, 4, 1),
        target_dte=3,
        dte_tolerance_days=1,
    )
    wrapped.list_available_expirations_by_type(
        entry_date=date(2025, 4, 1),
        contract_types=["call", "put"],
        expiration_dates=[date(2025, 4, 4)],
    )

    assert profiler.contract_fetch_ms == pytest.approx(9.0)
    assert profiler.contract_gateway_method_ms["list_available_expirations_by_type"] == pytest.approx(3.0)
    assert profiler.contract_gateway_method_ms["list_contracts"] == pytest.approx(2.0)
    assert profiler.contract_gateway_method_ms["list_contracts_for_preferred_common_expiration"] == pytest.approx(4.0)
    assert profiler.contract_gateway_method_calls == {
        "list_available_expirations_by_type": 1,
        "list_contracts": 1,
        "list_contracts_for_preferred_common_expiration": 1,
    }


def test_engine_run_position_sizing_timing_excludes_quote_series_attachment(monkeypatch) -> None:
    import backtestforecast.backtests.engine as engine_module

    class _Clock:
        def __init__(self) -> None:
            self.value = 0.0

        def perf_counter(self) -> float:
            return self.value

        def advance(self, seconds: float) -> None:
            self.value += seconds

    logged: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(
        engine_module,
        "logger",
        SimpleNamespace(
            info=lambda event, **kwargs: logged.append((event, kwargs)),
            warning=lambda *args, **kwargs: None,
            debug=lambda *args, **kwargs: None,
        ),
    )

    clock = _Clock()
    monkeypatch.setattr(engine_module._time, "perf_counter", clock.perf_counter)

    engine = OptionsBacktestEngine()
    candidate = OpenMultiLegPosition(
        display_ticker="AAPL 100C",
        strategy_type="long_call",
        underlying_symbol="AAPL",
        entry_date=date(2025, 9, 2),
        entry_index=0,
        quantity=1,
        dte_at_open=30,
        option_legs=[
            OpenOptionLeg(
                ticker="O:AAPL251017C00100000",
                contract_type="call",
                side=1,
                strike_price=100.0,
                expiration_date=date(2025, 10, 17),
                quantity_per_unit=1,
                entry_mid=2.0,
                last_mid=2.0,
                contract_multiplier=100.0,
            )
        ],
        stock_legs=[],
        scheduled_exit_date=date(2025, 9, 3),
        capital_required_per_unit=200.0,
        max_loss_per_unit=200.0,
        max_profit_per_unit=None,
        entry_reason="signal",
        entry_commission_total=Decimal("0"),
        detail_json={},
    )

    monkeypatch.setattr(
        engine,
        "_build_position_with_timing",
        lambda **kwargs: candidate if kwargs["bar_index"] == 0 else None,
    )

    def _fake_resolve_position_size(**kwargs):
        clock.advance(0.003)
        return 1

    monkeypatch.setattr(engine, "_resolve_position_size", _fake_resolve_position_size)
    monkeypatch.setattr(
        engine,
        "_attach_position_quote_series",
        lambda position, *, option_gateway, start_date, end_date: clock.advance(0.007),
    )

    bars = [
        SimpleNamespace(
            trade_date=date(2025, 9, 2),
            open_price=100.0,
            high_price=100.0,
            low_price=100.0,
            close_price=100.0,
            volume=1_000_000,
        ),
        SimpleNamespace(
            trade_date=date(2025, 9, 3),
            open_price=101.0,
            high_price=101.0,
            low_price=101.0,
            close_price=101.0,
            volume=1_000_000,
        ),
    ]

    class _Gateway:
        def get_quote(self, option_ticker, trade_date):
            price = 2.0 if trade_date == date(2025, 9, 2) else 3.0
            return OptionQuoteRecord(
                trade_date=trade_date,
                bid_price=price - 0.1,
                ask_price=price + 0.1,
                participant_timestamp=None,
            )

        def get_ex_dividend_dates(self, start_date, end_date):
            return set()

    result = engine.run(
        BacktestConfig(
            symbol="AAPL",
            strategy_type="long_call",
            start_date=date(2025, 9, 2),
            end_date=date(2025, 9, 2),
            target_dte=30,
            dte_tolerance_days=30,
            max_holding_days=1,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("5"),
            commission_per_contract=Decimal("0.65"),
            entry_rules=[],
        ),
        bars,
        set(),
        _Gateway(),
    )

    assert result.summary.trade_count == 1
    event_name, payload = logged[-1]
    assert event_name == "backtest.engine_run_timing"
    assert payload["position_sizing_ms"] == pytest.approx(3.0)
    assert payload["attach_quote_series_ms"] == pytest.approx(7.0)


def test_clone_position_template_shares_quote_series_but_copies_lane_state() -> None:
    quote_series = {
        "O:LEG1": {
            date(2025, 9, 2): OptionQuoteRecord(date(2025, 9, 2), 1.9, 2.1, None),
        },
        "O:LEG2": {
            date(2025, 9, 2): OptionQuoteRecord(date(2025, 9, 2), 0.9, 1.1, None),
        },
    }
    position = OpenMultiLegPosition(
        display_ticker="O:TEST",
        strategy_type="calendar_spread",
        underlying_symbol="AAPL",
        entry_date=date(2025, 9, 2),
        entry_index=0,
        quantity=1,
        dte_at_open=30,
        option_legs=[
            OpenOptionLeg("O:LEG1", "call", 1, 100.0, date(2025, 10, 17), 1, 2.0, 2.0),
            OpenOptionLeg("O:LEG2", "call", -1, 105.0, date(2025, 10, 17), 1, 1.0, 1.0),
        ],
        stock_legs=[
            OpenStockLeg("AAPL", 1, 100, 100.0, 100.0),
        ],
        scheduled_exit_date=date(2025, 9, 12),
        capital_required_per_unit=500.0,
        max_loss_per_unit=200.0,
        max_profit_per_unit=300.0,
        entry_reason="entry_rules_met",
        entry_commission_total=Decimal("1.30"),
        detail_json={"entry_underlying_close": 100.0},
        quote_series_lookup=quote_series,
        quote_series_loaded_tickers={"O:LEG1", "O:LEG2"},
    )

    clone = OptionsBacktestEngine._clone_position_template(position)

    assert clone is not position
    assert clone.quote_series_lookup is position.quote_series_lookup
    assert clone.quote_series_lookup is quote_series
    assert clone.quote_series_loaded_tickers == position.quote_series_loaded_tickers
    assert clone.quote_series_loaded_tickers is not position.quote_series_loaded_tickers
    assert clone.option_legs is not position.option_legs
    assert clone.option_legs[0] is not position.option_legs[0]
    assert clone.stock_legs is not position.stock_legs
    assert clone.stock_legs[0] is not position.stock_legs[0]
    assert clone.detail_json is not position.detail_json

    clone.quantity = 3
    clone.option_legs[0].last_mid = 9.9
    clone.stock_legs[0].last_price = 123.0
    clone.detail_json["entry_underlying_close"] = 101.0

    assert position.quantity == 1
    assert position.option_legs[0].last_mid == 2.0
    assert position.stock_legs[0].last_price == 100.0
    assert position.detail_json["entry_underlying_close"] == 100.0


def test_execution_service_passes_bundle_entry_rule_cache_to_supported_engines(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    settings = SimpleNamespace(
        option_cache_warn_age_seconds=259_200,
        backtest_option_prefetch_enabled=False,
        backtest_prefetch_min_trade_dates=2,
        backtest_prefetch_max_dates=4,
        backtest_prefetch_timeout_seconds=77,
    )

    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, "build_backtest_risk_free_rate_curve", lambda *args, **kwargs: None)

    bars = [
        SimpleNamespace(trade_date=date(2025, 4, 1)),
        SimpleNamespace(trade_date=date(2025, 4, 2)),
    ]
    bundle = HistoricalDataBundle(
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=SimpleNamespace(),
    )
    engine = _CapturingEngine()
    service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle),
        engine=engine,
    )
    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date="2025-04-01",
        end_date="2025-04-02",
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

    assert engine.shared_entry_rule_cache is bundle.entry_rule_cache


def test_execution_service_uses_multi_exit_engine_path_when_supported(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    settings = SimpleNamespace(
        option_cache_warn_age_seconds=259_200,
        backtest_option_prefetch_enabled=False,
        backtest_prefetch_min_trade_dates=2,
        backtest_prefetch_max_dates=4,
        backtest_prefetch_timeout_seconds=77,
    )

    monkeypatch.setattr(module, "get_settings", lambda: settings)
    monkeypatch.setattr(module, "build_backtest_risk_free_rate_curve", lambda *args, **kwargs: None)

    bars = [
        SimpleNamespace(trade_date=date(2025, 4, 1)),
        SimpleNamespace(trade_date=date(2025, 4, 2)),
    ]
    bundle = HistoricalDataBundle(
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=SimpleNamespace(),
    )
    engine = _CapturingEngine()
    service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle),
        engine=engine,
    )
    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date="2025-04-01",
        end_date="2025-04-02",
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

    results = service.execute_exit_policy_variants(
        request,
        exit_policies=[
            (Decimal("50"), None),
            (Decimal("75"), Decimal("100")),
        ],
        bundle=bundle,
        resolved_parameters=resolved,
    )

    assert [result.summary.ending_equity for result in results] == [10100.0, 10200.0]
    assert engine.shared_entry_rule_cache is bundle.entry_rule_cache
    assert engine.exit_variant_configs is not None
    assert [config.profit_target_pct for config in engine.exit_variant_configs] == [50.0, 75.0]
    assert [config.stop_loss_pct for config in engine.exit_variant_configs] == [None, 100.0]


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
        entry_trade_bars=None,
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


def test_execution_service_warms_entry_quotes_for_historical_targeted_prefetch(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    captured: dict[str, object] = {}

    def _fake_targeted_prewarm(
        request,
        *,
        bundle,
        include_quotes,
        max_dates,
        warm_future_quotes,
        entry_trade_bars=None,
    ):
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
    historical_gateway = HistoricalOptionGateway.__new__(HistoricalOptionGateway)
    bundle = HistoricalDataBundle(
        bars=bars,
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=historical_gateway,
    )
    service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle),
        engine=_CapturingEngine(),
    )
    request = CreateBacktestRunRequest(
        symbol="SPY",
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
    )
    resolved = ResolvedExecutionParameters(
        risk_free_rate=0.01,
        risk_free_rate_source="configured_fallback",
        risk_free_rate_field_name="yield_3_month",
        risk_free_rate_model="curve_default",
        dividend_yield=0.0,
        source_of_truth="test",
    )

    service.execute_request(request, resolved_parameters=resolved)

    assert captured["include_quotes"] is True
    assert captured["max_dates"] == 4
    assert captured["warm_future_quotes"] is False


def test_execution_service_prefetch_plan_uses_signal_dates(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    settings = SimpleNamespace(
        backtest_option_prefetch_enabled=True,
        backtest_prefetch_min_trade_dates=2,
        backtest_prefetch_max_dates=2,
    )
    bars = [
        SimpleNamespace(trade_date=date(2025, 4, 1), close_price=100.0, high_price=101.0, low_price=99.0, volume=1_000_000),
        SimpleNamespace(trade_date=date(2025, 4, 2), close_price=101.0, high_price=102.0, low_price=100.0, volume=1_000_000),
        SimpleNamespace(trade_date=date(2025, 4, 3), close_price=102.0, high_price=103.0, low_price=101.0, volume=1_000_000),
        SimpleNamespace(trade_date=date(2025, 4, 4), close_price=103.0, high_price=104.0, low_price=102.0, volume=1_000_000),
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
        symbol="SPY",
        strategy_type="covered_call",
        start_date="2025-04-01",
        end_date="2025-04-04",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
    )

    monkeypatch.setattr(
        module.EntryRuleEvaluator,
        "build_entry_allowed_mask",
        lambda self: [False, True, False, True],
    )

    plan = service._build_prefetch_plan(request, bundle, settings)

    assert plan is not None
    assert plan.trade_dates == (date(2025, 4, 2), date(2025, 4, 4))


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
        entry_trade_bars=None,
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
        entry_trade_bars=None,
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


def test_execution_service_prefetch_requests_reuse_equivalent_strategy_groups(monkeypatch) -> None:
    import backtestforecast.services.backtest_execution as module

    prewarm_calls: list[str] = []

    def _fake_targeted_prewarm(
        request,
        *,
        bundle,
        include_quotes,
        max_dates,
        warm_future_quotes,
        entry_trade_bars=None,
    ):
        prewarm_calls.append(request.strategy_type.value)
        return SimpleNamespace(
            to_dict=lambda: {"dates_processed": 2, "contracts_fetched": 10, "quotes_fetched": 0, "errors": []}
        )

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
    common_kwargs = dict(
        symbol="SPY",
        start_date="2025-04-01",
        end_date="2025-04-03",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[],
    )
    request_one = CreateBacktestRunRequest(strategy_type="bull_call_debit_spread", **common_kwargs)
    request_two = CreateBacktestRunRequest(strategy_type="bear_call_credit_spread", **common_kwargs)

    summary = service.prefetch_requests_with_shared_bundle([request_one, request_two], bundle=bundle)

    assert prewarm_calls == ["bull_call_debit_spread"]
    assert summary["prefetch_count"] == 1
    assert summary["skipped_count"] == 1
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


def test_build_contract_delta_lookup_reuses_cached_lookup_for_identical_chain_and_inputs():
    class _Gateway:
        def __init__(self) -> None:
            self.quote_calls: list[tuple[str, date]] = []

        def get_chain_delta_lookup(self, contracts):
            return {}

        def get_quote(self, option_ticker: str, trade_date: date):
            self.quote_calls.append((option_ticker, trade_date))
            return OptionQuoteRecord(trade_date=trade_date, bid_price=2.0, ask_price=2.2, participant_timestamp=None)

    gateway = _Gateway()
    contracts = [
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
        OptionContractRecord("O:AAPL250404C00210000", "call", date(2025, 4, 4), 210.0, 100.0),
    ]

    first = build_contract_delta_lookup(
        contracts=contracts,
        option_gateway=gateway,
        trade_date=date(2025, 4, 1),
        underlying_close=205.0,
        dte_days=3,
        risk_free_rate=0.01,
        dividend_yield=0.02,
    )
    second = build_contract_delta_lookup(
        contracts=contracts,
        option_gateway=gateway,
        trade_date=date(2025, 4, 1),
        underlying_close=205.0,
        dte_days=3,
        risk_free_rate=0.01,
        dividend_yield=0.02,
    )

    assert second == first
    assert gateway.quote_calls == [
        ("O:AAPL250404C00200000", date(2025, 4, 1)),
        ("O:AAPL250404C00210000", date(2025, 4, 1)),
    ]


def test_common_expiration_selection_cache_is_shared_across_gateways_with_shared_state():
    shared_state = object()

    class _Gateway:
        def __init__(self) -> None:
            self._shared_state = shared_state
            self.symbol = "AAPL"
            self.calls = 0

        def list_contracts_for_expirations(self, **kwargs):
            self.calls += 1
            expiration_dates = kwargs["expiration_dates"]
            contract_type = kwargs["contract_type"]
            first = expiration_dates[0]
            return {
                expiration: [
                    OptionContractRecord(
                        f"O:{contract_type[0].upper()}{idx}",
                        contract_type,
                        expiration,
                        100.0,
                        100.0,
                    )
                ] if expiration == first else []
                for idx, expiration in enumerate(expiration_dates)
            }

    with common_module._COMMON_EXPIRATION_SELECTION_CACHE_LOCK:
        common_module._COMMON_EXPIRATION_SELECTION_CACHE.clear()
    try:
        first = _Gateway()
        second = _Gateway()

        resolved_first = select_preferred_common_expiration_contracts(
            first,
            entry_date=date(2025, 4, 1),
            target_dte=3,
            dte_tolerance_days=1,
        )
        resolved_second = select_preferred_common_expiration_contracts(
            second,
            entry_date=date(2025, 4, 1),
            target_dte=3,
            dte_tolerance_days=1,
        )

        assert resolved_second == resolved_first
        assert first.calls == 2
        assert second.calls == 0
    finally:
        with common_module._COMMON_EXPIRATION_SELECTION_CACHE_LOCK:
            common_module._COMMON_EXPIRATION_SELECTION_CACHE.clear()


def test_select_preferred_common_expiration_contracts_uses_combined_batch_fetch_when_available():
    class _Gateway:
        def __init__(self) -> None:
            self.combined_calls = 0

        def list_contracts_for_expirations_by_type(self, **kwargs):
            self.combined_calls += 1
            expiration_dates = kwargs["expiration_dates"]
            first = expiration_dates[0]
            return {
                "call": {
                    expiration: [
                        OptionContractRecord(
                            f"O:C{idx}",
                            "call",
                            expiration,
                            100.0,
                            100.0,
                        )
                    ] if expiration == first else []
                    for idx, expiration in enumerate(expiration_dates)
                },
                "put": {
                    expiration: [
                        OptionContractRecord(
                            f"O:P{idx}",
                            "put",
                            expiration,
                            100.0,
                            100.0,
                        )
                    ] if expiration == first else []
                    for idx, expiration in enumerate(expiration_dates)
                },
            }

        def list_contracts_for_expirations(self, **kwargs):
            raise AssertionError("combined batch fetch should take priority")

    with common_module._COMMON_EXPIRATION_SELECTION_CACHE_LOCK:
        common_module._COMMON_EXPIRATION_SELECTION_CACHE.clear()
    try:
        gateway = _Gateway()
        expiration, call_contracts, put_contracts = select_preferred_common_expiration_contracts(
            gateway,
            entry_date=date(2025, 4, 1),
            target_dte=3,
            dte_tolerance_days=1,
        )

        assert expiration == date(2025, 4, 4)
        assert call_contracts[0].contract_type == "call"
        assert put_contracts[0].contract_type == "put"
        assert gateway.combined_calls == 1
    finally:
        with common_module._COMMON_EXPIRATION_SELECTION_CACHE_LOCK:
            common_module._COMMON_EXPIRATION_SELECTION_CACHE.clear()


def test_select_preferred_common_expiration_contracts_prefers_combined_batch_over_availability_probe():
    class _Gateway:
        def __init__(self) -> None:
            self.availability_calls = 0
            self.combined_expiration_calls: list[list[date]] = []

        def list_available_expirations_by_type(self, **kwargs):
            self.availability_calls += 1
            expiration_dates = kwargs["expiration_dates"]
            chosen = expiration_dates[1]
            return {
                "call": [chosen],
                "put": [chosen],
            }

        def list_contracts_for_expirations_by_type(self, **kwargs):
            expiration_dates = list(kwargs["expiration_dates"])
            self.combined_expiration_calls.append(expiration_dates)
            expiration = expiration_dates[1]
            return {
                "call": {
                    expiration: [
                        OptionContractRecord("O:C1", "call", expiration, 100.0, 100.0),
                    ],
                },
                "put": {
                    expiration: [
                        OptionContractRecord("O:P1", "put", expiration, 100.0, 100.0),
                    ],
                },
            }

        def list_contracts_for_expirations(self, **kwargs):
            raise AssertionError("combined by-type batch should take priority")

    with common_module._COMMON_EXPIRATION_SELECTION_CACHE_LOCK:
        common_module._COMMON_EXPIRATION_SELECTION_CACHE.clear()
    try:
        gateway = _Gateway()
        expiration, call_contracts, put_contracts = select_preferred_common_expiration_contracts(
            gateway,
            entry_date=date(2025, 4, 1),
            target_dte=3,
            dte_tolerance_days=1,
        )

        assert expiration == date(2025, 4, 5)
        assert gateway.availability_calls == 0
        assert gateway.combined_expiration_calls == [preferred_expiration_dates(date(2025, 4, 1), 3, 1)]
        assert call_contracts[0].contract_type == "call"
        assert put_contracts[0].contract_type == "put"
    finally:
        with common_module._COMMON_EXPIRATION_SELECTION_CACHE_LOCK:
            common_module._COMMON_EXPIRATION_SELECTION_CACHE.clear()


def test_resolve_strike_uses_tuple_delta_lookup_when_contracts_share_single_expiration():
    class _Gateway:
        def get_quote(self, option_ticker: str, trade_date: date):
            raise AssertionError("tuple-key delta lookup should avoid quote fallback")

    contracts = [
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
        OptionContractRecord("O:AAPL250404C00210000", "call", date(2025, 4, 4), 210.0, 100.0),
    ]
    selection = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))

    resolved = resolve_strike(
        [200.0, 210.0],
        205.0,
        "call",
        selection,
        dte_days=3,
        delta_lookup={
            (200.0, date(2025, 4, 4)): 0.31,
            (210.0, date(2025, 4, 4)): 0.18,
        },
        contracts=contracts,
        option_gateway=_Gateway(),
        trade_date=date(2025, 4, 1),
    )

    assert resolved == 200.0


def test_approx_bsm_delta_delegates_to_optional_native_kernel(monkeypatch):
    class _NativeKernel:
        def approx_bsm_delta(
            self,
            spot,
            strike,
            dte_days,
            contract_type,
            vol,
            risk_free_rate,
            dividend_yield,
        ):
            assert (spot, strike, dte_days, contract_type) == (100.0, 105.0, 30, "call")
            assert vol == 0.3
            return 0.1234

    monkeypatch.setattr(native_kernels_module, "_load_native_kernel_module", lambda: _NativeKernel())

    assert common_module._approx_bsm_delta(100.0, 105.0, 30, "call") == pytest.approx(0.1234)


def test_approx_bsm_delta_many_delegates_to_optional_native_kernel(monkeypatch):
    captured: dict[str, object] = {}

    class _NativeKernel:
        def approx_bsm_delta_many(
            self,
            spot,
            strikes,
            dte_days,
            contract_types,
            vols,
            risk_free_rate,
            dividend_yield,
        ):
            captured["spot"] = spot
            captured["strikes"] = list(strikes)
            captured["dte_days"] = dte_days
            captured["contract_types"] = list(contract_types)
            captured["vols"] = list(vols)
            captured["risk_free_rate"] = risk_free_rate
            captured["dividend_yield"] = dividend_yield
            return [0.31, 0.18]

    monkeypatch.setattr(native_kernels_module, "_load_native_kernel_module", lambda: _NativeKernel())

    resolved = native_kernels_module.approx_bsm_delta_many(
        205.0,
        [200.0, 210.0],
        3,
        ["call", "call"],
        [0.25, 0.25],
        risk_free_rate=0.01,
        dividend_yield=0.02,
    )

    assert resolved == [0.31, 0.18]
    assert captured == {
        "spot": 205.0,
        "strikes": [200.0, 210.0],
        "dte_days": 3,
        "contract_types": ["call", "call"],
        "vols": [0.25, 0.25],
        "risk_free_rate": 0.01,
        "dividend_yield": 0.02,
    }


def test_resolve_delta_target_strike_from_vols_delegates_to_optional_native_kernel(monkeypatch):
    captured: dict[str, object] = {}

    class _NativeKernel:
        def resolve_delta_target_strike_from_vols(
            self,
            spot,
            strikes,
            dte_days,
            contract_types,
            vols,
            target_delta,
            risk_free_rate,
            dividend_yield,
        ):
            captured["spot"] = spot
            captured["strikes"] = list(strikes)
            captured["dte_days"] = dte_days
            captured["contract_types"] = list(contract_types)
            captured["vols"] = list(vols)
            captured["target_delta"] = target_delta
            captured["risk_free_rate"] = risk_free_rate
            captured["dividend_yield"] = dividend_yield
            return strikes[0]

    monkeypatch.setattr(native_kernels_module, "_load_native_kernel_module", lambda: _NativeKernel())

    resolved = native_kernels_module.resolve_delta_target_strike_from_vols(
        205.0,
        [200.0, 210.0],
        3,
        ["call", "call"],
        [0.25, 0.25],
        0.30,
        risk_free_rate=0.01,
        dividend_yield=0.02,
    )

    assert resolved == 200.0
    assert captured == {
        "spot": 205.0,
        "strikes": [200.0, 210.0],
        "dte_days": 3,
        "contract_types": ["call", "call"],
        "vols": [0.25, 0.25],
        "target_delta": 0.30,
        "risk_free_rate": 0.01,
        "dividend_yield": 0.02,
    }


def test_build_contract_delta_lookup_batches_optional_native_kernel_calls(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_many(
        spot,
        strikes,
        dte_days,
        contract_types,
        vols,
        risk_free_rate,
        dividend_yield,
    ):
        captured["spot"] = spot
        captured["strikes"] = list(strikes)
        captured["dte_days"] = dte_days
        captured["contract_types"] = list(contract_types)
        captured["vols"] = list(vols)
        captured["risk_free_rate"] = risk_free_rate
        captured["dividend_yield"] = dividend_yield
        return [0.31, 0.18]

    monkeypatch.setattr(common_module, "_kernel_approx_bsm_delta_many", _fake_many)
    monkeypatch.setattr(common_module, "_estimate_iv_for_contract", lambda *args, **kwargs: None)

    class _Gateway:
        def get_quote(self, option_ticker: str, trade_date: date):
            raise AssertionError("realized-vol batch path should not fetch quotes")

    contracts = [
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
        OptionContractRecord("O:AAPL250404C00210000", "call", date(2025, 4, 4), 210.0, 100.0),
    ]

    lookup = build_contract_delta_lookup(
        contracts=contracts,
        option_gateway=_Gateway(),
        trade_date=date(2025, 4, 1),
        underlying_close=205.0,
        dte_days=3,
        risk_free_rate=0.01,
        dividend_yield=0.02,
        realized_vol=0.25,
    )

    assert lookup == {
        (200.0, date(2025, 4, 4)): 0.31,
        (210.0, date(2025, 4, 4)): 0.18,
    }
    assert captured == {
        "spot": 205.0,
        "strikes": [200.0, 210.0],
        "dte_days": 3,
        "contract_types": ["call", "call"],
        "vols": [0.25, 0.25],
        "risk_free_rate": 0.01,
        "dividend_yield": 0.02,
    }


def test_build_contract_delta_lookup_batches_iv_quote_fetch_when_available(monkeypatch):
    import backtestforecast.backtests.rules as rules_module

    captured: dict[str, object] = {}

    def _fake_many(
        spot,
        strikes,
        dte_days,
        contract_types,
        vols,
        risk_free_rate,
        dividend_yield,
    ):
        captured["spot"] = spot
        captured["strikes"] = list(strikes)
        captured["dte_days"] = dte_days
        captured["contract_types"] = list(contract_types)
        captured["vols"] = list(vols)
        captured["risk_free_rate"] = risk_free_rate
        captured["dividend_yield"] = dividend_yield
        return [0.31, 0.18]

    monkeypatch.setattr(common_module, "_kernel_approx_bsm_delta_many", _fake_many)
    monkeypatch.setattr(rules_module, "implied_volatility_from_price", lambda **kwargs: 0.22)

    class _Gateway:
        def __init__(self) -> None:
            self.batch_calls: list[tuple[list[str], date]] = []

        def get_iv(self, key):
            return False, None

        def store_iv(self, key, value):
            return None

        def get_quotes(self, option_tickers, trade_date):
            self.batch_calls.append((list(option_tickers), trade_date))
            return {
                ticker: OptionQuoteRecord(trade_date=trade_date, bid_price=2.0, ask_price=2.2, participant_timestamp=None)
                for ticker in option_tickers
            }

        def get_quote(self, option_ticker, trade_date):
            raise AssertionError("batch IV quote fetch should avoid single-quote calls")

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
        dividend_yield=0.02,
    )

    assert lookup == {
        (200.0, date(2025, 4, 4)): 0.31,
        (210.0, date(2025, 4, 4)): 0.18,
    }
    assert gateway.batch_calls == [
        (["O:AAPL250404C00200000", "O:AAPL250404C00210000"], date(2025, 4, 1))
    ]
    assert captured == {
        "spot": 205.0,
        "strikes": [200.0, 210.0],
        "dte_days": 3,
        "contract_types": ["call", "call"],
        "vols": [0.22, 0.22],
        "risk_free_rate": 0.01,
        "dividend_yield": 0.02,
    }


def test_native_kernel_loader_falls_back_to_ctypes_library_when_python_module_missing(monkeypatch):
    class _NativeKernel:
        pass

    monkeypatch.setattr(native_kernels_module.importlib, "import_module", lambda name: (_ for _ in ()).throw(ImportError()))
    monkeypatch.setattr(native_kernels_module, "_load_ctypes_native_kernel", lambda: _NativeKernel())
    native_kernels_module.reset_native_kernel_module_cache()

    first = native_kernels_module._load_native_kernel_module()
    second = native_kernels_module._load_native_kernel_module()

    assert isinstance(first, _NativeKernel)
    assert second is first


def test_resolve_strike_delta_target_delegates_final_choice_to_optional_native_kernel(monkeypatch):
    captured: dict[str, object] = {}

    class _NativeKernel:
        def choose_delta_target_strike(self, strikes, deltas, target_delta):
            captured["strikes"] = list(strikes)
            captured["deltas"] = list(deltas)
            captured["target_delta"] = target_delta
            return strikes[-1]

    monkeypatch.setattr(native_kernels_module, "_MIN_NATIVE_CHOOSE_DELTA_TARGET_STRIKE_LEN", 1)
    monkeypatch.setattr(native_kernels_module, "_load_native_kernel_module", lambda: _NativeKernel())
    selection = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))

    resolved = resolve_strike(
        [200.0, 210.0],
        205.0,
        "call",
        selection,
        dte_days=3,
        delta_lookup={
            (200.0, date(2025, 4, 4)): 0.31,
            (210.0, date(2025, 4, 4)): 0.18,
        },
        contracts=[
            OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
            OptionContractRecord("O:AAPL250404C00210000", "call", date(2025, 4, 4), 210.0, 100.0),
        ],
        trade_date=date(2025, 4, 1),
    )

    assert resolved == 210.0
    assert captured == {
        "strikes": [200.0, 210.0],
        "deltas": [0.31, 0.18],
        "target_delta": pytest.approx(0.30),
    }


def test_choose_delta_target_strike_uses_optional_native_kernel_for_short_lists_when_extension_loaded(monkeypatch):
    class _NativeKernel:
        def choose_delta_target_strike(self, strikes, deltas, target_delta):
            assert strikes == [200.0, 210.0]
            assert deltas == [0.31, 0.18]
            assert target_delta == pytest.approx(0.30)
            return strikes[-1]

    monkeypatch.setattr(native_kernels_module, "_load_native_kernel_module", lambda: _NativeKernel())

    assert native_kernels_module.choose_delta_target_strike([200.0, 210.0], [0.31, 0.18], 0.30) == 210.0


def test_resolve_strike_uses_python_choice_for_short_lists_with_ctypes_wrapper(monkeypatch):
    class _NativeKernel:
        source = "dummy.dll"

        def choose_delta_target_strike(self, strikes, deltas, target_delta):
            raise AssertionError("short strike lists should stay on the Python chooser")

    monkeypatch.setattr(native_kernels_module, "_load_native_kernel_module", lambda: _NativeKernel())
    selection = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))

    resolved = resolve_strike(
        [200.0, 210.0],
        205.0,
        "call",
        selection,
        dte_days=3,
        delta_lookup={
            (200.0, date(2025, 4, 4)): 0.31,
            (210.0, date(2025, 4, 4)): 0.18,
        },
        contracts=[
            OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
            OptionContractRecord("O:AAPL250404C00210000", "call", date(2025, 4, 4), 210.0, 100.0),
        ],
        trade_date=date(2025, 4, 1),
    )

    assert resolved == 200.0


def test_resolve_strike_uses_combined_native_resolver_when_all_strikes_need_approximation(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_resolve(
        spot,
        strikes,
        dte_days,
        contract_types,
        vols,
        target_delta,
        risk_free_rate,
        dividend_yield,
    ):
        captured["spot"] = spot
        captured["strikes"] = list(strikes)
        captured["dte_days"] = dte_days
        captured["contract_types"] = contract_types
        captured["vols"] = list(vols)
        captured["target_delta"] = target_delta
        captured["risk_free_rate"] = risk_free_rate
        captured["dividend_yield"] = dividend_yield
        return strikes[0]

    monkeypatch.setattr(common_module, "_kernel_resolve_delta_target_strike_from_vols", _fake_resolve)
    monkeypatch.setattr(
        common_module,
        "_kernel_approx_bsm_delta_many",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("combined native resolver should bypass batch delta output")),
    )
    monkeypatch.setattr(
        common_module,
        "_kernel_choose_delta_target_strike",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("combined native resolver should bypass Python-side choose handoff")),
    )
    selection = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))

    resolved = resolve_strike(
        [200.0, 210.0],
        205.0,
        "call",
        selection,
        dte_days=3,
        realized_vol=0.25,
        risk_free_rate=0.01,
    )

    assert resolved == 200.0
    assert captured == {
        "spot": 205.0,
        "strikes": [200.0, 210.0],
        "dte_days": 3,
        "contract_types": "call",
        "vols": [0.25, 0.25],
        "target_delta": 0.3,
        "risk_free_rate": 0.01,
        "dividend_yield": 0.0,
    }


def test_resolve_strike_batches_iv_quote_fetch_when_available(monkeypatch):
    import backtestforecast.backtests.rules as rules_module

    captured: dict[str, object] = {}

    def _fake_resolve(
        spot,
        strikes,
        dte_days,
        contract_types,
        vols,
        target_delta,
        risk_free_rate,
        dividend_yield,
    ):
        captured["spot"] = spot
        captured["strikes"] = list(strikes)
        captured["dte_days"] = dte_days
        captured["contract_types"] = contract_types
        captured["vols"] = list(vols)
        captured["target_delta"] = target_delta
        captured["risk_free_rate"] = risk_free_rate
        captured["dividend_yield"] = dividend_yield
        return strikes[0]

    monkeypatch.setattr(common_module, "_kernel_resolve_delta_target_strike_from_vols", _fake_resolve)
    monkeypatch.setattr(rules_module, "implied_volatility_from_price", lambda **kwargs: 0.24)
    selection = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))

    class _Gateway:
        def __init__(self) -> None:
            self.batch_calls: list[tuple[list[str], date]] = []

        def get_iv(self, key):
            return False, None

        def store_iv(self, key, value):
            return None

        def get_quotes(self, option_tickers, trade_date):
            self.batch_calls.append((list(option_tickers), trade_date))
            return {
                ticker: OptionQuoteRecord(trade_date=trade_date, bid_price=2.0, ask_price=2.2, participant_timestamp=None)
                for ticker in option_tickers
            }

        def get_quote(self, option_ticker, trade_date):
            raise AssertionError("batch IV quote fetch should avoid single-quote calls")

    gateway = _Gateway()
    contracts = [
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
        OptionContractRecord("O:AAPL250404C00210000", "call", date(2025, 4, 4), 210.0, 100.0),
    ]

    resolved = resolve_strike(
        [200.0, 210.0],
        205.0,
        "call",
        selection,
        dte_days=3,
        contracts=contracts,
        option_gateway=gateway,
        trade_date=date(2025, 4, 1),
        risk_free_rate=0.01,
    )

    assert resolved == 200.0
    assert gateway.batch_calls == [
        (["O:AAPL250404C00200000", "O:AAPL250404C00210000"], date(2025, 4, 1))
    ]
    assert captured == {
        "spot": 205.0,
        "strikes": [200.0, 210.0],
        "dte_days": 3,
        "contract_types": "call",
        "vols": [0.24, 0.24],
        "target_delta": 0.3,
        "risk_free_rate": 0.01,
        "dividend_yield": 0.0,
    }


def test_resolve_strike_batches_fallback_delta_calculations(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_many(
        spot,
        strikes,
        dte_days,
        contract_types,
        vols,
        risk_free_rate,
        dividend_yield,
    ):
        captured["spot"] = spot
        captured["strikes"] = list(strikes)
        captured["dte_days"] = dte_days
        captured["contract_types"] = contract_types
        captured["vols"] = list(vols)
        captured["risk_free_rate"] = risk_free_rate
        captured["dividend_yield"] = dividend_yield
        return [0.18]

    def _fake_choose(strikes, deltas, target_delta):
        captured["choose_strikes"] = list(strikes)
        captured["choose_deltas"] = list(deltas)
        captured["target_delta"] = target_delta
        return strikes[0]

    monkeypatch.setattr(common_module, "_kernel_approx_bsm_delta_many", _fake_many)
    monkeypatch.setattr(common_module, "_kernel_choose_delta_target_strike", _fake_choose)
    selection = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))

    resolved = resolve_strike(
        [200.0, 210.0],
        205.0,
        "call",
        selection,
        dte_days=3,
        delta_lookup={
            (200.0, date(2025, 4, 4)): 0.31,
        },
        contracts=[
            OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
            OptionContractRecord("O:AAPL250404C00210000", "call", date(2025, 4, 4), 210.0, 100.0),
        ],
        trade_date=date(2025, 4, 1),
        realized_vol=0.25,
        risk_free_rate=0.01,
    )

    assert resolved == 200.0
    assert captured == {
        "spot": 205.0,
        "strikes": [210.0],
        "dte_days": 3,
        "contract_types": "call",
        "vols": [0.25],
        "risk_free_rate": 0.01,
        "dividend_yield": 0.0,
        "choose_strikes": [200.0, 210.0],
        "choose_deltas": [0.31, 0.18],
        "target_delta": 0.3,
    }


def test_calendar_group_cache_is_shared_across_gateways_with_shared_state():
    shared_state = object()

    class _Gateway:
        def __init__(self) -> None:
            self._shared_state = shared_state
            self.symbol = "AAPL"
            self.calls = 0

        def list_contracts_for_expirations(self, **kwargs):
            self.calls += 1
            expiration_dates = kwargs["expiration_dates"]
            near = expiration_dates[0]
            far = expiration_dates[-1]
            contract_type = kwargs["contract_type"]
            return {
                expiration: [
                    OptionContractRecord(
                        f"O:{contract_type[0].upper()}{idx}",
                        contract_type,
                        expiration,
                        100.0,
                        100.0,
                    )
                ] if expiration in {near, far} else []
                for idx, expiration in enumerate(expiration_dates)
            }

    with calendar_module._CALENDAR_GROUP_CACHE_LOCK:
        calendar_module._CALENDAR_GROUP_CACHE.clear()
    try:
        first = _Gateway()
        second = _Gateway()

        resolved_first = resolve_calendar_contract_groups(
            first,
            entry_date=date(2025, 4, 1),
            contract_type="call",
            target_dte=7,
            dte_tolerance_days=2,
        )
        resolved_second = resolve_calendar_contract_groups(
            second,
            entry_date=date(2025, 4, 1),
            contract_type="call",
            target_dte=7,
            dte_tolerance_days=2,
        )

        assert resolved_second == resolved_first
        assert first.calls == 1
        assert second.calls == 0
    finally:
        with calendar_module._CALENDAR_GROUP_CACHE_LOCK:
            calendar_module._CALENDAR_GROUP_CACHE.clear()


def test_select_preferred_expiration_contracts_preserves_gateway_contract_sequence_identity():
    cached_contracts = [
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
        OptionContractRecord("O:AAPL250404C00210000", "call", date(2025, 4, 4), 210.0, 100.0),
    ]

    class _Gateway:
        def list_contracts_for_preferred_expiration(self, **kwargs):
            return cached_contracts

    expiration, returned_contracts = select_preferred_expiration_contracts(
        _Gateway(),
        entry_date=date(2025, 4, 1),
        contract_type="call",
        target_dte=3,
        dte_tolerance_days=1,
    )

    assert expiration == date(2025, 4, 4)
    assert returned_contracts is cached_contracts


def test_common_sorted_expirations_reuses_sorted_common_dates_and_filters_minimum():
    left_contracts = [
        OptionContractRecord("O:L1", "call", date(2025, 4, 11), 100.0, 100.0),
        OptionContractRecord("O:L2", "call", date(2025, 4, 18), 100.0, 100.0),
        OptionContractRecord("O:L3", "call", date(2025, 4, 25), 100.0, 100.0),
    ]
    right_contracts = [
        OptionContractRecord("O:R1", "put", date(2025, 4, 4), 100.0, 100.0),
        OptionContractRecord("O:R2", "put", date(2025, 4, 18), 100.0, 100.0),
        OptionContractRecord("O:R3", "put", date(2025, 4, 25), 100.0, 100.0),
    ]

    assert common_sorted_expirations(left_contracts, right_contracts) == [
        date(2025, 4, 18),
        date(2025, 4, 25),
    ]
    assert common_sorted_expirations(
        left_contracts,
        right_contracts,
        min_expiration_exclusive=date(2025, 4, 18),
    ) == [date(2025, 4, 25)]


def test_choose_primary_expiration_date_prefers_closest_not_before_target():
    resolved = choose_primary_expiration_date(
        [
            date(2025, 4, 11),
            date(2025, 4, 18),
            date(2025, 4, 25),
        ],
        entry_date=date(2025, 4, 1),
        target_dte=14,
    )

    assert resolved == date(2025, 4, 18)


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
