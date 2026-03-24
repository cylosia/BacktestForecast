from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import BacktestExecutionResult, EquityPointResult, TradeResult
from backtestforecast.db.base import Base
from backtestforecast.market_data.service import HistoricalDataBundle
from backtestforecast.models import User
from backtestforecast.schemas.backtests import CreateBacktestRunRequest
from backtestforecast.services.backtest_execution import BacktestExecutionService
from backtestforecast.services.backtests import BacktestService
from tests.conftest import strip_partial_indexes_for_sqlite


class _StubMarketDataService:
    def __init__(
        self,
        bundle: HistoricalDataBundle,
        *,
        treasury_rate: float | None = None,
        treasury_series: dict[date, float] | None = None,
    ) -> None:
        self._bundle = bundle
        self.treasury_calls: list[tuple[date, date, str | None]] = []
        self.treasury_series_calls: list[tuple[date, date, str | None]] = []

        def _get_average_treasury_yield(start_date, end_date, field_name=None):
            self.treasury_calls.append((start_date, end_date, field_name))
            return treasury_rate

        def _get_treasury_yield_series(start_date, end_date, field_name=None):
            self.treasury_series_calls.append((start_date, end_date, field_name))
            return dict(treasury_series or {})

        self.client = SimpleNamespace(
            close=lambda: None,
            get_average_treasury_yield=_get_average_treasury_yield,
            get_treasury_yield_series=_get_treasury_yield_series,
        )

    def prepare_backtest(self, request: CreateBacktestRunRequest) -> HistoricalDataBundle:
        return self._bundle


class _CapturingEngine:
    def __init__(self) -> None:
        self.last_config = None

    def run(self, config, bars, earnings_dates, ex_dividend_dates, option_gateway) -> BacktestExecutionResult:
        self.last_config = config
        trades = _sample_trades(config.symbol, config.strategy_type)
        equity_curve = _sample_equity_curve()
        summary = build_summary(
            10_000.0,
            float(equity_curve[-1].equity),
            trades,
            equity_curve,
            risk_free_rate=config.risk_free_rate,
            risk_free_rate_curve=config.risk_free_rate_curve,
        )
        return BacktestExecutionResult(summary=summary, trades=trades, equity_curve=equity_curve)


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    strip_partial_indexes_for_sqlite(engine)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture()
def user(db_session):
    user = User(clerk_user_id="rfr_override_user", email="rfr-override@example.com")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


def _sample_trades(symbol: str, strategy_type: str) -> list[TradeResult]:
    start = date(2024, 1, 2)
    pnls = [Decimal("120"), Decimal("-80"), Decimal("90"), Decimal("-40"), Decimal("60")]
    trades: list[TradeResult] = []
    for idx, pnl in enumerate(pnls):
        trades.append(
            TradeResult(
                option_ticker=f"O:{symbol}{idx}",
                strategy_type=strategy_type,
                underlying_symbol=symbol,
                entry_date=start + timedelta(days=idx * 7),
                exit_date=start + timedelta(days=idx * 7 + 2),
                expiration_date=start + timedelta(days=idx * 7 + 30),
                quantity=1,
                dte_at_open=30,
                holding_period_days=2,
                entry_underlying_close=Decimal("100"),
                exit_underlying_close=Decimal("101"),
                entry_mid=Decimal("2.00"),
                exit_mid=Decimal("3.00"),
                gross_pnl=pnl,
                net_pnl=pnl,
                total_commissions=Decimal("1.00"),
                entry_reason="entry_rules_met",
                exit_reason="target_hit",
            )
        )
    return trades


def _sample_equity_curve() -> list[EquityPointResult]:
    start = date(2024, 1, 2)
    equities = [
        Decimal("10000"), Decimal("10080"), Decimal("10030"), Decimal("10140"), Decimal("10090"),
        Decimal("10180"), Decimal("10110"), Decimal("10220"), Decimal("10170"), Decimal("10290"),
        Decimal("10210"), Decimal("10320"), Decimal("10270"), Decimal("10380"), Decimal("10310"),
        Decimal("10410"), Decimal("10360"), Decimal("10480"), Decimal("10410"), Decimal("10530"),
        Decimal("10460"), Decimal("10590"), Decimal("10510"), Decimal("10630"), Decimal("10560"),
        Decimal("10690"), Decimal("10620"), Decimal("10740"), Decimal("10680"), Decimal("10810"),
        Decimal("10730"), Decimal("10860"), Decimal("10790"), Decimal("10920"), Decimal("10840"),
    ]
    points: list[EquityPointResult] = []
    peak = equities[0]
    for idx, equity in enumerate(equities):
        if equity > peak:
            peak = equity
        drawdown_pct = (peak - equity) / peak * Decimal("100") if peak else Decimal("0")
        points.append(
            EquityPointResult(
                trade_date=start + timedelta(days=idx),
                equity=equity,
                cash=equity,
                position_value=Decimal("0"),
                drawdown_pct=drawdown_pct,
            )
        )
    return points


@pytest.mark.target_assertion
def test_request_risk_free_rate_override_takes_precedence_and_round_trips(
    monkeypatch, db_session, user, target_assertion
):
    override_rate = Decimal("0.0123")
    estimated_rate = 0.099
    settings = SimpleNamespace(app_env="test", risk_free_rate=0.045, option_cache_warn_age_seconds=259_200)

    monkeypatch.setattr("backtestforecast.services.backtest_execution.get_settings", lambda: settings)
    monkeypatch.setattr("backtestforecast.services.backtests.get_settings", lambda: settings)
    monkeypatch.setattr("backtestforecast.backtests.types.estimate_risk_free_rate", lambda *_: estimated_rate)

    bundle = HistoricalDataBundle(bars=[], earnings_dates=set(), ex_dividend_dates=set(), option_gateway=SimpleNamespace())
    engine = _CapturingEngine()
    execution_service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle),
        engine=engine,
    )

    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date="2024-01-02",
        end_date="2024-03-31",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
        risk_free_rate=override_rate,
    )

    execution_result = execution_service.execute_request(request)

    expected_override = build_summary(
        10_000.0,
        float(execution_result.equity_curve[-1].equity),
        execution_result.trades,
        execution_result.equity_curve,
        risk_free_rate=float(override_rate),
    )
    expected_estimated = build_summary(
        10_000.0,
        float(execution_result.equity_curve[-1].equity),
        execution_result.trades,
        execution_result.equity_curve,
        risk_free_rate=estimated_rate,
    )

    target_assertion()
    assert engine.last_config is not None
    assert engine.last_config.risk_free_rate == pytest.approx(float(override_rate))
    assert execution_result.summary.sharpe_ratio == pytest.approx(expected_override.sharpe_ratio)
    assert execution_result.summary.sharpe_ratio != pytest.approx(expected_estimated.sharpe_ratio)

    backtest_service = BacktestService(db_session, execution_service=execution_service)
    run = backtest_service.create_and_run(user, request)
    detail = backtest_service.get_run_for_owner(user_id=user.id, run_id=run.id)

    assert float(detail.summary.sharpe_ratio) == pytest.approx(expected_override.sharpe_ratio, abs=1e-4)
    assert detail.risk_free_rate == override_rate
    assert detail.risk_free_rate_model == "scalar"
    assert run.input_snapshot_json["resolved_risk_free_rate_source"] == "request_override"


@pytest.mark.target_assertion
def test_server_default_is_replaced_by_massive_treasury_rate(monkeypatch, db_session, user, target_assertion):
    treasury_rate = 0.051
    settings = SimpleNamespace(app_env="test", risk_free_rate=0.045, option_cache_warn_age_seconds=259_200)

    monkeypatch.setattr("backtestforecast.services.backtest_execution.get_settings", lambda: settings)
    monkeypatch.setattr("backtestforecast.services.backtests.get_settings", lambda: settings)
    monkeypatch.setattr("backtestforecast.backtests.run_warnings.get_settings", lambda: settings)
    monkeypatch.setattr(
        "backtestforecast.services.risk_free_rate.get_settings",
        lambda: settings,
    )

    bundle = HistoricalDataBundle(bars=[], earnings_dates=set(), ex_dividend_dates=set(), option_gateway=SimpleNamespace())
    engine = _CapturingEngine()
    execution_service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle, treasury_rate=treasury_rate),
        engine=engine,
    )

    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date="2024-01-02",
        end_date="2024-03-31",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
    )

    backtest_service = BacktestService(db_session, execution_service=execution_service)
    run = backtest_service.create_and_run(user, request)
    detail = backtest_service.get_run_for_owner(user_id=user.id, run_id=run.id)

    target_assertion()
    assert engine.last_config is not None
    assert engine.last_config.risk_free_rate == pytest.approx(treasury_rate)
    assert execution_service.market_data_service.treasury_calls == [
        (request.start_date, request.start_date, "yield_3_month")
    ]
    assert execution_service.market_data_service.treasury_series_calls == [
        (request.start_date, request.end_date, "yield_3_month"),
        (request.start_date, request.end_date, "yield_3_month"),
    ]
    assert float(detail.risk_free_rate) == pytest.approx(treasury_rate)
    assert detail.risk_free_rate_model == "curve_default"
    assert run.input_snapshot_json["resolved_risk_free_rate_source"] == "massive_treasury"
    assert run.input_snapshot_json["resolved_risk_free_rate_model"] == "curve_default"
    warning_codes = {warning.code for warning in detail.warnings}
    assert "historical_treasury_risk_free_rate" in warning_codes


def test_treasury_curve_is_used_for_later_date_excess_returns(monkeypatch):
    settings = SimpleNamespace(app_env="test", risk_free_rate=0.045, option_cache_warn_age_seconds=259_200)
    monkeypatch.setattr("backtestforecast.services.backtest_execution.get_settings", lambda: settings)
    monkeypatch.setattr("backtestforecast.services.risk_free_rate.get_settings", lambda: settings)

    bundle = HistoricalDataBundle(bars=[], earnings_dates=set(), ex_dividend_dates=set(), option_gateway=SimpleNamespace())
    curve = {
        date(2024, 1, 2): 0.01,
        date(2024, 1, 20): 0.08,
    }
    engine = _CapturingEngine()
    execution_service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle, treasury_rate=0.01, treasury_series=curve),
        engine=engine,
    )
    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date="2024-01-02",
        end_date="2024-03-31",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
    )

    result = execution_service.execute_request(request)

    expected_curve = build_summary(
        10_000.0,
        float(result.equity_curve[-1].equity),
        result.trades,
        result.equity_curve,
        risk_free_rate=0.01,
        risk_free_rate_curve=engine.last_config.risk_free_rate_curve,
    )
    expected_static = build_summary(
        10_000.0,
        float(result.equity_curve[-1].equity),
        result.trades,
        result.equity_curve,
        risk_free_rate=0.01,
    )

    assert engine.last_config is not None
    assert engine.last_config.risk_free_rate_curve is not None
    assert engine.last_config.risk_free_rate_curve.rate_for(date(2024, 2, 1)) == pytest.approx(0.08)
    assert result.summary.sharpe_ratio == pytest.approx(expected_curve.sharpe_ratio)
    assert result.summary.sharpe_ratio != pytest.approx(expected_static.sharpe_ratio)


def test_enqueue_audits_execution_parameter_resolution(monkeypatch, db_session, user):
    treasury_rate = 0.031
    settings = SimpleNamespace(app_env="test", risk_free_rate=0.045, option_cache_warn_age_seconds=259_200)

    monkeypatch.setattr("backtestforecast.services.backtest_execution.get_settings", lambda: settings)
    monkeypatch.setattr("backtestforecast.services.backtests.get_settings", lambda: settings)

    bundle = HistoricalDataBundle(bars=[], earnings_dates=set(), ex_dividend_dates=set(), option_gateway=SimpleNamespace())
    execution_service = BacktestExecutionService(
        market_data_service=_StubMarketDataService(bundle, treasury_rate=treasury_rate),
        engine=_CapturingEngine(),
    )

    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date="2024-01-02",
        end_date="2024-03-31",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
    )

    service = BacktestService(db_session, execution_service=execution_service)
    recorded: list[dict] = []
    monkeypatch.setattr(service.audit, "record_always", lambda **kwargs: recorded.append(kwargs))

    run = service.enqueue(user, request)

    assert run.input_snapshot_json["resolved_risk_free_rate_source"] == "configured_fallback"
    event = next(event for event in recorded if event["event_type"] == "backtest.execution_parameters_resolved")
    assert event["metadata"]["risk_free_rate"] == settings.risk_free_rate
    assert event["metadata"]["risk_free_rate_source"] == "configured_fallback"


def test_enqueue_defers_massive_lookup_until_worker_execution(monkeypatch, db_session, user):
    treasury_rate = 0.031
    settings = SimpleNamespace(app_env="test", risk_free_rate=0.045, option_cache_warn_age_seconds=259_200)

    monkeypatch.setattr("backtestforecast.services.backtest_execution.get_settings", lambda: settings)
    monkeypatch.setattr("backtestforecast.services.backtests.get_settings", lambda: settings)
    monkeypatch.setattr("backtestforecast.backtests.run_warnings.get_settings", lambda: settings)
    monkeypatch.setattr("backtestforecast.services.risk_free_rate.get_settings", lambda: settings)

    bundle = HistoricalDataBundle(bars=[], earnings_dates=set(), ex_dividend_dates=set(), option_gateway=SimpleNamespace())
    market_data = _StubMarketDataService(bundle, treasury_rate=treasury_rate)
    execution_service = BacktestExecutionService(
        market_data_service=market_data,
        engine=_CapturingEngine(),
    )

    request = CreateBacktestRunRequest(
        symbol="AAPL",
        strategy_type="long_call",
        start_date="2024-01-02",
        end_date="2024-03-31",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
        entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
    )

    service = BacktestService(db_session, execution_service=execution_service)
    run = service.enqueue(user, request)

    assert market_data.treasury_calls == []
    assert float(run.risk_free_rate) == pytest.approx(settings.risk_free_rate)
    assert run.input_snapshot_json["resolved_risk_free_rate_source"] == "configured_fallback"

    executed = service.execute_run_by_id(run.id)

    assert executed.status == "succeeded"
    assert market_data.treasury_calls == [(request.start_date, request.start_date, "yield_3_month")]
    assert float(executed.risk_free_rate) == pytest.approx(treasury_rate)
    assert executed.input_snapshot_json["resolved_risk_free_rate_source"] == "massive_treasury"
