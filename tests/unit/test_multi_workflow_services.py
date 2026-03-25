from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.market_data.types import DailyBar, OptionContractRecord, OptionQuoteRecord
from backtestforecast.models import Base, User
from backtestforecast.schemas.backtests import RsiRule, StrategyType
from backtestforecast.schemas.multi_step_backtests import (
    CreateMultiStepRunRequest,
    StepContractSelection,
    StepTriggerDefinition,
    WorkflowStepDefinition,
)
from backtestforecast.schemas.multi_symbol_backtests import (
    CreateMultiSymbolRunRequest,
    MultiSymbolDefinition,
    MultiSymbolLegDefinition,
    MultiSymbolPriceRule,
    MultiSymbolStrategyGroup,
)
from backtestforecast.services.multi_step_backtests import MultiStepBacktestService
from backtestforecast.services.multi_symbol_backtests import MultiSymbolBacktestService
from tests.conftest import strip_partial_indexes_for_sqlite as _strip_partial_indexes_for_sqlite


class _FakeClient:
    def __init__(self, bars_by_symbol: dict[str, list[DailyBar]]) -> None:
        self._bars_by_symbol = bars_by_symbol

    def list_option_contracts(self, symbol: str, as_of_date: date, contract_type: str, expiration_gte: date, expiration_lte: date):
        expirations = [as_of_date + timedelta(days=7), as_of_date + timedelta(days=28)]
        return [
            OptionContractRecord(
                ticker=f"O:{symbol}{expiration.strftime('%y%m%d')}{contract_type[0].upper()}00100000",
                contract_type=contract_type,
                expiration_date=expiration,
                strike_price=100.0,
                shares_per_contract=100,
            )
            for expiration in expirations
        ]

    def get_option_quote_for_date(self, option_ticker: str, trade_date: date):
        day_offset = max((trade_date - date(2024, 1, 2)).days, 0)
        mid = 2.0 + (day_offset * 0.05)
        return OptionQuoteRecord(trade_date=trade_date, bid_price=mid - 0.05, ask_price=mid + 0.05, participant_timestamp=None)

    def list_ex_dividend_dates(self, symbol: str, start_date: date, end_date: date):
        return set()


class _FakeMarketDataService:
    def __init__(self, bars_by_symbol: dict[str, list[DailyBar]]) -> None:
        self.client = _FakeClient(bars_by_symbol)
        self._bars_by_symbol = bars_by_symbol
        self._redis_cache = None

    def _fetch_bars_coalesced(self, symbol: str, start: date, end: date):
        return [bar for bar in self._bars_by_symbol[symbol] if start <= bar.trade_date <= end]

    def _validate_bars(self, raw_bars, symbol: str):
        return list(raw_bars)

    def _load_ex_dividend_dates(self, symbol: str, *, start_date: date, end_date: date):
        return set()


class _FakeExecutionService:
    def __init__(self, bars_by_symbol: dict[str, list[DailyBar]]) -> None:
        self.market_data_service = _FakeMarketDataService(bars_by_symbol)

    def close(self) -> None:
        return None


def _session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    _strip_partial_indexes_for_sqlite(engine)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)()


def _bars(start: date, count: int, close_start: float) -> list[DailyBar]:
    return [
        DailyBar(
            trade_date=start + timedelta(days=idx),
            open_price=close_start + idx,
            high_price=close_start + idx + 1,
            low_price=close_start + idx - 1,
            close_price=close_start + idx,
            volume=1_000_000 + (idx * 10_000),
        )
        for idx in range(count)
    ]


def test_multi_symbol_service_executes_grouped_trades() -> None:
    session = _session()
    user = User(clerk_user_id="user_ms", email="multi@example.com")
    session.add(user)
    session.commit()

    bars_by_symbol = {
        "AAA": _bars(date(2024, 1, 2), 40, 100.0),
        "BBB": _bars(date(2024, 1, 2), 40, 105.0),
    }
    service = MultiSymbolBacktestService(session, execution_service=_FakeExecutionService(bars_by_symbol))
    request = CreateMultiSymbolRunRequest(
        name="alpha",
        symbols=[
            MultiSymbolDefinition(symbol="AAA", risk_per_trade_pct=Decimal("2")),
            MultiSymbolDefinition(symbol="BBB", risk_per_trade_pct=Decimal("2")),
        ],
        strategy_groups=[
            MultiSymbolStrategyGroup(
                name="pair",
                synchronous_entry=True,
                legs=[
                    MultiSymbolLegDefinition(symbol="AAA", strategy_type=StrategyType.LONG_CALL, target_dte=14, dte_tolerance_days=3, max_holding_days=5, quantity_mode="fixed_contracts", fixed_contracts=1),
                    MultiSymbolLegDefinition(symbol="BBB", strategy_type=StrategyType.LONG_CALL, target_dte=14, dte_tolerance_days=3, max_holding_days=5, quantity_mode="fixed_contracts", fixed_contracts=1),
                ],
            )
        ],
        entry_rules=[MultiSymbolPriceRule(left_symbol="AAA", left_indicator="close", operator="gt", threshold=Decimal("99"))],
        start_date=date(2024, 1, 5),
        end_date=date(2024, 1, 25),
        account_size=Decimal("100000"),
        commission_per_contract=Decimal("0.65"),
    )
    run = service.enqueue(user, request)
    run = service.execute_run_by_id(run.id)
    assert run.status == "succeeded"
    detail = service.get_run_for_owner(user_id=user.id, run_id=run.id)
    assert detail.trade_groups
    assert len(detail.trade_groups[0].trades) == 2


def test_multi_step_service_executes_multiple_steps() -> None:
    session = _session()
    user = User(clerk_user_id="user_mt", email="step@example.com")
    session.add(user)
    session.commit()

    bars_by_symbol = {"SPY": _bars(date(2024, 1, 2), 50, 100.0)}
    service = MultiStepBacktestService(session, execution_service=_FakeExecutionService(bars_by_symbol))
    request = CreateMultiStepRunRequest(
        name="workflow",
        symbol="SPY",
        workflow_type="sequential",
        start_date=date(2024, 1, 5),
        end_date=date(2024, 2, 10),
        account_size=Decimal("100000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("0.65"),
        initial_entry_rules=[RsiRule(type="rsi", operator="gt", threshold=Decimal("0"), period=2)],
        steps=[
            WorkflowStepDefinition(
                step_number=1,
                name="Open calendar",
                action="open_position",
                trigger=StepTriggerDefinition(mode="rule_match", rules=[RsiRule(type="rsi", operator="gt", threshold=Decimal("0"), period=2)]),
                contract_selection=StepContractSelection(strategy_type=StrategyType.CALENDAR_SPREAD, target_dte=7, dte_tolerance_days=3, max_holding_days=10),
            ),
            WorkflowStepDefinition(
                step_number=2,
                name="Resell weekly premium",
                action="sell_premium",
                trigger=StepTriggerDefinition(mode="after_expiration", require_prior_step_status="expired"),
                contract_selection=StepContractSelection(strategy_type=StrategyType.CALENDAR_SPREAD, target_dte=7, dte_tolerance_days=3, max_holding_days=10),
            ),
        ],
    )
    run = service.enqueue(user, request)
    run = service.execute_run_by_id(run.id)
    assert run.status == "succeeded"
    detail = service.get_run_for_owner(user_id=user.id, run_id=run.id)
    assert len(detail.trades) >= 2
    assert any(event.step_number == 2 for event in detail.events)


def test_multi_step_close_position_triggers_from_prior_step_execution() -> None:
    session = _session()
    user = User(clerk_user_id="user_close", email="close@example.com")
    session.add(user)
    session.commit()

    bars_by_symbol = {"SPY": _bars(date(2024, 1, 2), 30, 100.0)}
    service = MultiStepBacktestService(session, execution_service=_FakeExecutionService(bars_by_symbol))
    request = CreateMultiStepRunRequest(
        name="close-workflow",
        symbol="SPY",
        workflow_type="close-sequence",
        start_date=date(2024, 1, 5),
        end_date=date(2024, 1, 25),
        account_size=Decimal("100000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("0.65"),
        initial_entry_rules=[RsiRule(type="rsi", operator="gt", threshold=Decimal("0"), period=2)],
        steps=[
            WorkflowStepDefinition(
                step_number=1,
                name="Open long call",
                action="open_position",
                trigger=StepTriggerDefinition(mode="rule_match", rules=[RsiRule(type="rsi", operator="gt", threshold=Decimal("0"), period=2)]),
                contract_selection=StepContractSelection(strategy_type=StrategyType.LONG_CALL, target_dte=14, dte_tolerance_days=3, max_holding_days=10),
            ),
            WorkflowStepDefinition(
                step_number=2,
                name="Close inventory",
                action="close_position",
                trigger=StepTriggerDefinition(mode="date_offset", days_after_prior_step=2),
                contract_selection=StepContractSelection(strategy_type=StrategyType.LONG_CALL, target_dte=14, dte_tolerance_days=3, max_holding_days=10),
            ),
        ],
    )
    run = service.enqueue(user, request)
    run = service.execute_run_by_id(run.id)
    assert run.status == "succeeded"
    detail = service.get_run_for_owner(user_id=user.id, run_id=run.id)
    assert any(step.step_number == 2 and step.status == "executed" for step in detail.steps)
    assert detail.trades


def test_multi_step_roll_transition_executes() -> None:
    session = _session()
    user = User(clerk_user_id="user_roll", email="roll@example.com")
    session.add(user)
    session.commit()

    bars_by_symbol = {"SPY": _bars(date(2024, 1, 2), 40, 100.0)}
    service = MultiStepBacktestService(session, execution_service=_FakeExecutionService(bars_by_symbol))
    request = CreateMultiStepRunRequest(
        name="roll-workflow",
        symbol="SPY",
        workflow_type="roll-sequence",
        start_date=date(2024, 1, 5),
        end_date=date(2024, 2, 5),
        account_size=Decimal("100000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("0.65"),
        initial_entry_rules=[RsiRule(type="rsi", operator="gt", threshold=Decimal("0"), period=2)],
        steps=[
            WorkflowStepDefinition(
                step_number=1,
                name="Open call",
                action="open_position",
                trigger=StepTriggerDefinition(mode="rule_match", rules=[RsiRule(type="rsi", operator="gt", threshold=Decimal("0"), period=2)]),
                contract_selection=StepContractSelection(strategy_type=StrategyType.LONG_CALL, target_dte=14, dte_tolerance_days=3, max_holding_days=10),
            ),
            WorkflowStepDefinition(
                step_number=2,
                name="Roll call",
                action="roll",
                trigger=StepTriggerDefinition(mode="date_offset", days_after_prior_step=2),
                contract_selection=StepContractSelection(strategy_type=StrategyType.LONG_CALL, target_dte=14, dte_tolerance_days=3, max_holding_days=10),
            ),
        ],
    )
    run = service.enqueue(user, request)
    run = service.execute_run_by_id(run.id)
    assert run.status == "succeeded"
    detail = service.get_run_for_owner(user_id=user.id, run_id=run.id)
    assert any(step.step_number == 2 and step.status == "executed" for step in detail.steps)
    assert len(detail.trades) >= 1


def test_multi_step_hedge_transition_executes_overlay() -> None:
    session = _session()
    user = User(clerk_user_id="user_hedge", email="hedge@example.com")
    session.add(user)
    session.commit()

    bars_by_symbol = {"SPY": _bars(date(2024, 1, 2), 40, 100.0)}
    service = MultiStepBacktestService(session, execution_service=_FakeExecutionService(bars_by_symbol))
    request = CreateMultiStepRunRequest(
        name="hedge-workflow",
        symbol="SPY",
        workflow_type="hedge-sequence",
        start_date=date(2024, 1, 5),
        end_date=date(2024, 2, 5),
        account_size=Decimal("100000"),
        risk_per_trade_pct=Decimal("2"),
        commission_per_contract=Decimal("0.65"),
        initial_entry_rules=[RsiRule(type="rsi", operator="gt", threshold=Decimal("0"), period=2)],
        steps=[
            WorkflowStepDefinition(
                step_number=1,
                name="Open call",
                action="open_position",
                trigger=StepTriggerDefinition(mode="rule_match", rules=[RsiRule(type="rsi", operator="gt", threshold=Decimal("0"), period=2)]),
                contract_selection=StepContractSelection(strategy_type=StrategyType.LONG_CALL, target_dte=14, dte_tolerance_days=3, max_holding_days=12),
            ),
            WorkflowStepDefinition(
                step_number=2,
                name="Overlay put hedge",
                action="hedge",
                trigger=StepTriggerDefinition(mode="date_offset", days_after_prior_step=1),
                contract_selection=StepContractSelection(strategy_type=StrategyType.LONG_PUT, target_dte=14, dte_tolerance_days=3, max_holding_days=8),
            ),
        ],
    )
    run = service.enqueue(user, request)
    run = service.execute_run_by_id(run.id)
    assert run.status == "succeeded"
    detail = service.get_run_for_owner(user_id=user.id, run_id=run.id)
    assert any(step.step_number == 2 and step.status == "executed" for step in detail.steps)
    assert detail.trades
