from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import (
    BacktestConfig,
    BacktestExecutionResult,
    EquityPointResult,
)
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
