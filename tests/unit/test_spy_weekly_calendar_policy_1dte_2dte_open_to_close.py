from __future__ import annotations

import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import pytest

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.strategies.registry import STRATEGY_REGISTRY
from backtestforecast.backtests.types import BacktestConfig, OpenMultiLegPosition, OpenOptionLeg
from scripts.spy_weekly_calendar_policy_1dte_2dte_open_to_close_common import (  # noqa: E402
    OPEN_TO_CLOSE_EXIT_REASON,
    _build_strategy_sets,
    shift_indicator_rows_to_entry_dates,
    simulate_intraday_open_to_close_trade,
)


def test_shift_indicator_rows_to_entry_dates_uses_prior_trading_day() -> None:
    shifted = shift_indicator_rows_to_entry_dates(
        indicators_by_date={
            date(2025, 1, 2): {"roc63": 1.0},
            date(2025, 1, 3): {"roc63": 2.0},
            date(2025, 1, 6): {"roc63": 3.0},
        },
        entry_dates=[
            date(2025, 1, 2),
            date(2025, 1, 3),
            date(2025, 1, 6),
            date(2025, 1, 7),
        ],
    )

    assert date(2025, 1, 2) not in shifted
    assert shifted[date(2025, 1, 3)]["roc63"] == 1.0
    assert shifted[date(2025, 1, 6)]["roc63"] == 2.0
    assert shifted[date(2025, 1, 7)]["roc63"] == 3.0


def test_build_strategy_sets_omits_profit_target_dimension_for_open_close_variant() -> None:
    bullish, bearish, neutral = _build_strategy_sets("SPY")

    assert len(bullish) == 2
    assert len(bearish) == 6
    assert len(neutral) == 2
    assert {strategy.profit_target_pct for strategy in bullish + bearish + neutral} == {0}
    assert bullish[0].label == "spy_call_d40_o2c1"
    assert bearish[-1].label == "bear_spy_put_d50_o2c1"
    assert neutral[-1].label == "neutral_spy_call_d50_o2c1"


class _StubIntradayQuoteCache:
    def __init__(self, quotes_by_ticker):
        self._quotes_by_ticker = quotes_by_ticker

    def get_regular_session_quotes(self, option_ticker: str, trade_date: date):
        return list(self._quotes_by_ticker[option_ticker])

    def get_open_quote(self, option_ticker: str, trade_date: date):
        quotes = self.get_regular_session_quotes(option_ticker, trade_date)
        return quotes[0] if quotes else None

    def get_close_quote(self, option_ticker: str, trade_date: date):
        quotes = self.get_regular_session_quotes(option_ticker, trade_date)
        return quotes[-1] if quotes else None


class _TestIntradayStrategy:
    def build_position(self, config: BacktestConfig, bar, bar_index: int, option_gateway):
        quotes = option_gateway.get_quotes(["O:SHORT", "O:LONG"], bar.trade_date)
        short_quote = quotes["O:SHORT"]
        long_quote = quotes["O:LONG"]
        assert short_quote is not None
        assert long_quote is not None
        entry_package_value = (long_quote.mid_price - short_quote.mid_price) * 100.0
        return OpenMultiLegPosition(
            display_ticker="TEST_INTRADAY_PACKAGE",
            strategy_type=config.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=0,
            quantity=1,
            dte_at_open=1,
            option_legs=[
                OpenOptionLeg(
                    ticker="O:SHORT",
                    contract_type="call",
                    side=-1,
                    strike_price=100.0,
                    expiration_date=date(2026, 1, 9),
                    quantity_per_unit=1,
                    entry_mid=short_quote.mid_price,
                    last_mid=short_quote.mid_price,
                ),
                OpenOptionLeg(
                    ticker="O:LONG",
                    contract_type="call",
                    side=1,
                    strike_price=100.0,
                    expiration_date=date(2026, 1, 16),
                    quantity_per_unit=1,
                    entry_mid=long_quote.mid_price,
                    last_mid=long_quote.mid_price,
                ),
            ],
            scheduled_exit_date=date(2026, 1, 16),
            capital_required_per_unit=200.0,
            max_loss_per_unit=200.0,
            max_profit_per_unit=None,
            entry_reason="test_entry",
            entry_commission_total=Decimal("0"),
            detail_json={
                "entry_package_market_value": entry_package_value,
                "capital_required_per_unit": 200.0,
                "legs": [],
            },
        )


def _config() -> BacktestConfig:
    return BacktestConfig(
        symbol="SPY",
        strategy_type="test_intraday_strategy",
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 5),
        target_dte=1,
        dte_tolerance_days=0,
        max_holding_days=1,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0"),
        entry_rules=[],
    )


@pytest.fixture(autouse=True)
def _register_test_strategy(monkeypatch):
    monkeypatch.setitem(STRATEGY_REGISTRY, "test_intraday_strategy", _TestIntradayStrategy())


def test_simulate_intraday_open_to_close_trade_exits_on_stop_loss() -> None:
    trade = simulate_intraday_open_to_close_trade(
        strategy=SimpleNamespace(label="test_intraday"),
        config=_config(),
        bar=SimpleNamespace(
            trade_date=date(2026, 1, 5),
            open_price=100.0,
            close_price=101.0,
            volume=1.0,
        ),
        contract_gateway=SimpleNamespace(),
        intraday_quote_cache=_StubIntradayQuoteCache(
            {
                "O:SHORT": [
                    SimpleNamespace(mid_price=1.0, participant_timestamp=10),
                    SimpleNamespace(mid_price=1.0, participant_timestamp=20),
                ],
                "O:LONG": [
                    SimpleNamespace(mid_price=2.0, participant_timestamp=10),
                    SimpleNamespace(mid_price=1.5, participant_timestamp=20),
                ],
            }
        ),
        engine=OptionsBacktestEngine(),
        stop_loss_pct=10.0,
    )

    assert trade is not None
    assert trade.exit_reason == "stop_loss"
    assert trade.detail_json["intraday_exit_mode"] == "intraday_threshold"
    assert trade.detail_json["intraday_exit_participant_timestamp"] == 20
    assert float(trade.net_pnl) < 0


def test_simulate_intraday_open_to_close_trade_uses_regular_session_close_when_no_stop() -> None:
    trade = simulate_intraday_open_to_close_trade(
        strategy=SimpleNamespace(label="test_intraday"),
        config=_config(),
        bar=SimpleNamespace(
            trade_date=date(2026, 1, 5),
            open_price=100.0,
            close_price=101.0,
            volume=1.0,
        ),
        contract_gateway=SimpleNamespace(),
        intraday_quote_cache=_StubIntradayQuoteCache(
            {
                "O:SHORT": [
                    SimpleNamespace(mid_price=1.0, participant_timestamp=10),
                    SimpleNamespace(mid_price=0.9, participant_timestamp=30),
                ],
                "O:LONG": [
                    SimpleNamespace(mid_price=2.0, participant_timestamp=10),
                    SimpleNamespace(mid_price=2.2, participant_timestamp=30),
                ],
            }
        ),
        engine=OptionsBacktestEngine(),
        stop_loss_pct=10.0,
    )

    assert trade is not None
    assert trade.exit_reason == OPEN_TO_CLOSE_EXIT_REASON
    assert trade.detail_json["intraday_exit_mode"] == "regular_session_close_quote"
    assert trade.detail_json["exit_quote_participant_timestamps"] == {"O:SHORT": 30, "O:LONG": 30}
    assert float(trade.net_pnl) > 0
